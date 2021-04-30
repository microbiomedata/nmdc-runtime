from fastapi import APIRouter

router = APIRouter()


@router.post("/jobs")
def create_job():
    pass


@router.get("/jobs")
def list_jobs():
    pass


@router.get("/jobs/{job_id}")
def get_job():
    pass


@router.patch("/jobs/{job_id}")
def update_job():
    pass


@router.post("/jobs/{job_id}:run")
def run_job():
    pass


@router.get("/jobs/{job_id}/executions")
def list_job_executions():
    pass


@router.get("/jobs/{job_id}/executions/{exec_id}")
def get_job_execution():
    pass
