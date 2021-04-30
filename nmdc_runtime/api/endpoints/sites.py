from fastapi import APIRouter

router = APIRouter()


@router.post("/sites")
def create_site():
    pass


@router.get("/sites")
def list_sites():
    pass


@router.get("/sites/{site_id}")
def get_site():
    pass


@router.patch("/sites/{site_id}")
def update_site():
    pass


@router.put("/sites/{site_id}")
def replace_site():
    pass


@router.delete("/sites/{site_id}")
def delete_site():
    pass
