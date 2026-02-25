from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

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

@app.get("/", response_class=HTMLResponse)
@app.get("/posts", response_class=HTMLResponse)
def home():
    return f"<h1>{posts[0]['title']}</h1>"

@app.get("/api/posts")
def get_posts():
    return posts