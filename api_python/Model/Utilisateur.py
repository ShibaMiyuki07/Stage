from pydantic import BaseModel


class Utilisateur(BaseModel):
    username : str
    password : str