# Identifier Minting and Administration

How does identifier minting and administration work in NMDC? 

## Minting

A *minting request* has these attributes:

- `service`: a reference to a minter service, e.g. the central minter service
- `requester`: a reference to a requesting agent, e.g. a person
- `schema_class`: a reference to a NMDC Schema class, e.g. [nmdc:Biosample](https://w3id.org/nmdc/Biosample).
- `how_many`: how many new `schema_class` identifiers the `requester` wants `service` to mint

Assuming the `requester` is authenticated and is authorized by the `service` to mint identifiers,
one or more *draft identifiers* are minted. Each minted identifier has these attributes:

- `name`: a literal string value that adheres to the scheme of [nmdc:id](https://w3id.org/nmdc/id), e.g. "nmdc:bsm-11-abc123".
- `typecode`: a reference to the ID's typecode component, e.g. with `name` "bsm" and `schema_class` [nmdc:Biosample](https://w3id.org/nmdc/Biosample).
- `shoulder`: a reference to the ID's shoulder component, e.g. with `name` "11" and `assigned_to` the `service` that minted the ID.
- `status`:  a reference to the ID's status, e.g. "draft".

```mermaid
flowchart LR
    %% o1[/"(input/output) object"/]
    %% s1["(process) step"]
    %% t1(["terminator (process start/end)"])
    %% d1{decision}
    %% b1[("(data)base")]
    %% m1[\manual step/]
    
    %% p1[[predefined step]]
    classDef literal stroke-dasharray:1 4;
    classDef user stroke-dasharray:8 8;
    
    
    %% request-handler user-story model-diagram

    o_typecodename[/"#quot;bsm#quot;"/]:::literal
    o_idname[/"#quot;nmdc:bsm-11-abc123#quot;"/]:::literal
    o_shouldername[/"#quot;11#quot;"/]:::literal
    o_howmany[/"1"/]:::literal
    o_request[/"minting request"/]:::user
    o_shoulder[/"11"/]
    o_typecode[/"bsm"/]
    o_schemaclass[/"nmdc:Biosample"/]
    o_service[/"Central Minting Service"/]
    o_requester[/Alicia/]
    o_id_draft1[/draft 1 of ID/]
    o_draft[/draft/]

    s_minting[minting]

    o_typecode-. name .->o_typecodename
    o_typecode-. schema_class .->o_schemaclass

    click o_schemaclass href "https://microbiomedata.github.io/nmdc-schema/Biosample"

    o_request-. service .->o_service
    o_request-. requester .->o_requester
    o_request-. schema_class .->o_schemaclass
    o_request-. how_many .->o_howmany
    
    o_request-->|starts|s_minting[minting]

    o_shoulder-. assigned_to .-> o_service
    o_shoulder.->|name|o_shouldername

    s_minting-- draft_identifier -->o_id_draft1
    o_id_draft1-. name .->o_idname
    o_id_draft1-. shoulder .->o_shoulder
    o_id_draft1-. typecode .-> o_typecode
    o_id_draft1-. status .-> o_draft
```

All typecodes and shoulders are sourced from the database.

All minted identifiers are persisted to the database.