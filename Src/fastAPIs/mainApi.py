from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

posts: list[dict] = [
    {
        "id" : 1,
        "author": "Scott Mejlænder Lasrsen",
        "title": "Test av FastAPI",
        "content": "bare tull",
    },
    {
        "id": 2,
        "author": "Erling Haaland",
        "title" : "fotball spiller spiss",
        "content" : "masse mål",
    }, 
]

@app.get("/", include_in_schema=False)
@app.get("/posts", include_in_schema=False)
def home(request: Request):
    return templates.TemplateResponse(request, "home.html", {"posts":posts, "title": "Home"})

@app.get("/api/posts")
def get_posts():
    return posts