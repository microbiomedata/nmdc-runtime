# User Journeys

Re: "do workflows need to know NMDC specifics", I've drafted a set of user journey diagrams with
"Workflow" and "NMDC" as actors.


The first, "Keep NMDC in the know", is the current intended Workflow executioner journey, and is
implemented by the NMDC Runtime:

```mermaid
journey
    title Keep NMDC in the know
    section Ensure and claim job
      Register metadata for inputs: 2: Workflow, NMDC
      Spawn job to be done: 3: NMDC
      Claim advertised job: 3: Workflow, NMDC
    section Job executiom
      Execute workflow: 5: Workflow
      Register execution status: 3: Workflow, NMDC
    section Output metadata registration
      Register data objects: 2: Workflow, NMDC
      Register other metadata: 2: Workflow, NMDC
```

The second journey, "Tell NMDC after-the-fact", is also currently supported by the implementation,
and is similar to the first journey, but there is no real-time provenance / status update reporting:

```mermaid
journey
    title Tell NMDC after-the-fact
    section Ensure and claim job
      Register metadata for inputs: 2: Workflow, NMDC
      Spawn job to be done: 3: NMDC
      Claim advertised job: 3: Workflow, NMDC
    section Output metadata registration
      Register completed execution: 3: Workflow, NMDC
      Register data objects: 2: Workflow, NMDC
      Register other metadata: 2: Workflow, NMDC
```



The third journey, "Discover via NMDC, and give back", is aspirational. In current practice,
Workflow executioners already know what they want to do, already know the relevant input data
objects / metadata, and are simply informing the Runtime of what they did / are doing in the
language of the NMDC schema. This last journey reflects some feedback we've gotten. For example,
folks would like to register transformation functions with NMDC so that NMDC can e.g. generate
schema-complaint JSON from a workflow's native GFF output files:

```mermaid
journey
    title Discover via NMDC, and give back
    section Create job to be done
      Sense relevant metadata updates: 3: NMDC
      Spawn and advertise job to be done: 3: NMDC
    section Job execution
      Register job-started status: 5: Workflow, NMDC
      Execute workflow, register status updates: 5: Workflow, NMDC
    section Output metadata registration
      Register data objects: 2: Workflow, NMDC
      Register some other metadata: 2: Workflow, NMDC
    section Additional metadata generation
      Sense relevant new data objects: 3: NMDC
      Generate schema-compliant JSON: 3: NMDC
```