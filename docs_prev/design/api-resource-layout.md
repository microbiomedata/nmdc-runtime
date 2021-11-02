The syntax in this document for endpoint specs is as the following:
```python
@get("/operations", ListOperationsResponse[ResultT, MetadataT])
def list_operations(req: ListOperationsRequest): pass
```
This syntax is meant to resemble a real FastAPI endpoint signature, e.g.
```python
@get("/operations", response_model=ListOperationsResponse[ResultT, MetadataT])
def list_operations(req: ListOperationsRequest = Depends()):
    pass
```

The syntax for data models is meant to resemble a real Pydantic model, with some liberties taken for
brevity:
- for generic models, sublassing `GenericModel` is implicit and omitted.
- for models that only inherit from `BaseModel`, this is implicit and omitted.

This design is an adaptation of patterns described in [Geewax, *API Design Patterns*. Manning,
2021.](https://www.manning.com/books/api-design-patterns)

# Long-running operations

```python
@get("/operations/{op_id}", Operation[ResultT, MetadataT])
def get_operation(op_id: str): pass

@get("/operations", ListOperationsResponse[ResultT, MetadataT])
def list_operations(req: ListOperationsRequest): pass

@post("/operations/{op_id}:wait", Operation[ResultT, MetadataT])
def wait_operation(op_id: str): pass

@post("/operations/{op_id}:cancel", Operation[ResultT, MetadataT])
def cancel_operation(op_id: str): pass

@post("/operations/{op_id}:pause", Operation[ResultT, MetadataT])
def pause_operation(op_id: str): pass

@post("/operations/{op_id}:resume", Operation[ResultT, MetadataT])
def resume_operation(op_id: str): pass

class Operation(Generic[ResultT, MetadataT]):
    id: str
    done: bool
    expire_time: datetime.datetime
    result: Optional[Union[ResultT, OperationError]]
    metadata: Optional[MetadataT]
    
class OperationError:
    code: str
    message: str
    details: Any

class ListOperationsRequest:
    filter: str
    max_page_size: int
    page_token: str

class ListOperationsResponse(Generic[ResultT, MetadataT]):
    resources: List[Operation[ResultT, MetadataT]]
    next_page_token: str
```

For an operation to be pausable and resumable, the metadata type (`MetadataT`) must have a `paused:
bool` field.

# Importing and Exporting

```python
@post("/documents/{doc_type}:export", Operation[ExportDocsResponse, ExportDocsMetadata])
def export_documents(req: ExportDocsRequest): pass

@post("/documents/{doc_type}:import", Operation[ImportDocsResponse, ImportDocsMetadata])
def import_documents(req: ImportDocsRequest): pass

class ExportDocsRequest:
    doc_type: str
    output_config: DocsOutputConfig
    data_destination: DataDestination
    filter: str

class ExportDocsResponse:
    doc_type: str
    docs_exported: int
    # ...

class ExportDocsMetadata:
    doc_type: str
    docs_exported: int
    # ...
    
class DocsOutputConfig:
    # content type for serialization: "json", "csv", etc.
    content_type: Optional[str]
    # Use ${number} for zero-padded file ID number.
    # Content type will be appended with file extension, e.g. ".json"
    # Default: "{doc_type}s-part-${number}"
    filename_template: Optional[str]
    # Default: no max file size
    max_file_size_MB: Optional[int]
    # E.g. "zip", "bz2". Default no compression
    compression_format: Optional[str]
    
class DataDestination:
    # Unique ID of destination type, e.g. "s3", "nersc cfs".
    type: str
    
class S3Destination(DataDestination):
    type = "s3"
    bucket_id: str
    object_prefix: Optional[str]
    
class ImportDocumentsRequest:
    doc_type: str
    input_config: DocsInputConfig
    data_source: DataSource

class ImportDocsResponse:
    doc_type: str
    docs_imported: int

class ImportDocsMetadata:
    doc_type: str
    docs_imported: int
    
class DocsInputConfig:
    content_type: Optional[str]  # default auto-detected
    compression_format: Optional[str]  # default not compressed
    
class DataSource:
    type: str
    
class S3Source(DataSource):
    type = "s3"
    bucked_id: str
    # One or more masks in standard glob format, e.g. "folder/samples.*.csv"
    mask = Union[List[str], str]
```
