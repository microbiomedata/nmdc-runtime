from fastapi import APIRouter

router = APIRouter()


@router.get("/jobs")
def list_jobs():
    pass


@router.get("/jobs/{job_id}")
def get_job():
    pass


@router.patch("/jobs/{job_id}")
def update_job():
    pass


@router.post("/jobs/{job_id}:claim")
def claim_job():
    pass


@router.get(
    "/jobs/{job_id}/executions",
    description=(
        "A sub-resource of a job resource, the result of a successful run of that job. "
        "An execution resource may be retrieved by any site; however, it may be created "
        "and updated only by the site that ran its job."
    ),
)
def list_job_executions():
    pass


@router.get("/jobs/{job_id}/executions/{exec_id}")
def get_job_execution():
    pass
