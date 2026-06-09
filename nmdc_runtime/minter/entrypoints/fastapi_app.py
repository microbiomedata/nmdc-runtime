import traceback

from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database as MongoDatabase
from starlette import status

from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.lib.workflow_executions import parse_workflow_execution_id
from nmdc_runtime.api.models.site import get_current_client_site, Site
from nmdc_runtime.minter.adapters.repository import MongoIDStore, MinterError
from nmdc_runtime.minter.config import minting_service_id
from nmdc_runtime.minter.domain.model import (
    Identifier,
    AuthenticatedMintingRequest,
    MintingRequest,
    Entity,
    ResolutionRequest,
    BindingRequest,
    AuthenticatedBindingRequest,
    AuthenticatedDeleteRequest,
    DeleteRequest,
    AuthenticatedWorkflowExecutionIdMintingRequest,
    WorkflowExecutionIdMintingRequest,
)
from nmdc_runtime.minter.lib.identifiers import get_typecode_from_id
from nmdc_runtime.minter.lib.schema import (
    does_class_uri_belong_to_concrete_subclass_of_workflow_execution,
    get_typecodes_compatible_with_schema_class,
    get_class_name_from_class_uri,
)

router = APIRouter()

service = Entity(id=minting_service_id())


@router.post("/mint")
def mint_ids(
    req_mint: AuthenticatedMintingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> list[str]:
    """Mint one or more (typed) persistent identifiers."""

    # Note: The schema requires that instances of classes that inherit from `WorkflowExecution`
    #       have `id` values that end with ".{integer}"; but the minter currently mints `id`s
    #       that lack that suffix. As a result, the `id`s minted by the minter in those cases
    #       are not schema compliant.
    # TODO: Consider documenting the above information in a user-facing way.

    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        minted = s.mint(
            MintingRequest(
                service=service,
                requester=requester,
                **req_mint.model_dump(),
            )
        )
        return [d.id for d in minted]
    except MinterError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=traceback.format_exc(),
        )


@router.post("/mint/workflow_execution_id")
def mint_workflow_execution_id(
    wfe_id_minting_req: AuthenticatedWorkflowExecutionIdMintingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> str:
    """
    Mint a (typed) persistent identifier for a workflow execution.

    The identifier will include a "dot integer" suffix and will have the same base as the
    specified existing identifier, or a unique base if no existing identifier was specified.
    Unlike the `/mint` endpoint, this endpoint only mints a single identifier per request.
    """
    class_uri_entity = wfe_id_minting_req.schema_class
    existing_id = wfe_id_minting_req.existing_id

    # Check whether a class URI was specified.
    if class_uri_entity.id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Class URI must be specified.",
        )
    else:
        class_name = get_class_name_from_class_uri(class_uri_entity.id)
        if class_name is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Class URI does not belong to a schema class.",
            )

    # Check whether the specified class URI references a concrete subclass of `WorkflowExecution`.
    if not does_class_uri_belong_to_concrete_subclass_of_workflow_execution(class_uri_entity.id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Class URI must belong to a concrete subclass of WorkflowExecution.",
        )

    # Check whether an `id` was specified.
    if isinstance(existing_id, str):
        # If the specified `id` lacks a typecode, abort instead of checking the database.
        typecode = get_typecode_from_id(existing_id)
        if typecode is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="The specified ID lacks a typecode, so the ID is invalid.",
            )
        else:
            # If the typecode is not compatible with the specified schema class, abort instead of checking the database.
            compatible_typecodes = get_typecodes_compatible_with_schema_class(class_name)
            if typecode not in compatible_typecodes:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"The specified ID is not compatible with the specified class.",
                )

        # Check whether the specified ID either (a) has been minted or (b) is in use in the `workflow_execution_set`
        # collection.
        #
        # Note: ID management in the Runtime seems to me to have been designed such that documents in schema-described
        #       collections (such as `workflow_execution_set`) always have `id` values that were generated by some
        #       minter instance. In practice, however, that is not always the case. That's because:
        #        1. Workflow automation maintainers have been _deriving_ new `id` values based upon minted `id` values
        #           (e.g. minting `nmdc:wfnom-00-foo` and, from that, deriving `nmdc:wfnom-00-foo.1`, the latter of
        #           which the minter is not aware of); and the Runtime API does not currently prohibit the usage of
        #           non-minted `id` values, as long as they are schema-compliant strings.
        #        2. Although not common, additional minter instances have been used to mint `id` values still being used
        #           by documents in schema-described collections. Those minter instances (even if they were still live)
        #           are isolated from one another, so a given minter instance doesn't have a "list" of all `id` values
        #           minted by all (or, indeed, any) other minter instances.
        #        As a result; to check whether a `WorkflowExecution` `id` value is in use, we check the minter's history
        #        (to see whether _we_ have already minted that `id` value, regardless of whether it is in use), and we
        #        check the `workflow_execution_set` collection (to see whether that `id` value is already in use,
        #        regardless of how it was generated).
        #
        minter_id_records = mdb.get_collection("minter.id_records")
        if minter_id_records.count_documents({"id": existing_id}, limit=1) == 0:
            workflow_execution_set = mdb.get_collection("workflow_execution_set")
            if workflow_execution_set.count_documents({"id": existing_id}, limit=1) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"The specified ID does not exist.",
                )

    # Checkpoint: At this point, we know that the class URI references a concrete subclass of `WorkflowExecution`. In
    #             addition, if an `id` was specified, we know that it exists and is compatible with that subclass.
    pass

    # The next step is to mint a new `id` value (having either a new base, or the same base as the specified `id`).
    requester = Entity(id=site.id)
    base_id, _ = parse_workflow_execution_id(raw_id=existing_id) if isinstance(existing_id, str) else None
    minting_request = WorkflowExecutionIdMintingRequest(
        service=service,
        requester=requester,
        schema_class=class_uri_entity,
        base_id=base_id,
    )
    id_store = MongoIDStore(mdb)
    identifier = id_store.mint_workflow_execution_id(minting_request=minting_request)
    id_value = identifier.id
    if id_value is None:
        raise HTTPException(
            status_code=status.HTTP_500_BAD_REQUEST,
            detail="Failed to mint identifier. Please try again."
        )
    return id_value

@router.get("/resolve/{id_name}")
def resolve_id(
    id_name: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> Identifier:
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    id_ = s.resolve(
        ResolutionRequest(service=service, requester=requester, id_name=id_name)
    )
    return raise404_if_none(id_)


@router.post("/bind")
def bind(
    req_bind: AuthenticatedBindingRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
) -> Identifier:
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        id_ = s.bind(
            BindingRequest(
                service=service,
                requester=requester,
                id_name=req_bind.id_name,
                metadata_record=req_bind.metadata_record,
            )
        )
        return raise404_if_none(id_)
    except Exception as e:
        if str(e) == f"ID {req_bind.id_name} is unknown":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=traceback.format_exc(),
            )


@router.post("/delete")
def delete(
    req_del: AuthenticatedDeleteRequest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """Resolve a (typed) persistent identifier."""
    s = MongoIDStore(mdb)
    requester = Entity(id=site.id)
    try:
        id_ = s.delete(
            DeleteRequest(
                service=service,
                requester=requester,
                id_name=req_del.id_name,
            )
        )
    except Exception as e:
        if str(e) == f"ID {req_del.id_name} is unknown":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        elif str(e) == "Status not 'draft'. Can't delete.":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=traceback.format_exc(),
            )
