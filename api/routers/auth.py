from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from api.dependencies import create_token, verify_token
from api.models.responses import TokenResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

USERS = {
    "admin": "meteo2025",
    "analyst": "tunisia123",
}

ROLES = {
    "admin": "admin",
    "analyst": "analyst",
}

@router.post("/login", response_model=TokenResponse)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    username = form_data.username
    password = form_data.password

    if username not in USERS or USERS[username] != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_token(username)
    return TokenResponse(access_token=token, token_type="bearer")

@router.get("/me")
def read_users_me(username: str = Depends(verify_token)):
    role = ROLES.get(username, "analyst")
    return {"username": username, "role": role}
