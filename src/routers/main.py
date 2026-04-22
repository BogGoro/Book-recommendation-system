import os

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse, JSONResponse, Response

from src.scripts import auth
from src.scripts.book import Book
from src.scripts.book_messages import BookMessages
from src.scripts.book_stats import BookStats
from src.scripts.exceptions import BadCredentials, ObjectNotFound, UsernameNotUnique
from src.constants import TOP_LIST

from src.scripts.book_list import BookList
from src.scripts.message import Message
from src.scripts.search import Search
from src.scripts.user_list import UserList
from src.scripts.user_stats import UserStats
from src.scripts.status import Status
from src.scripts.score import Score


router = APIRouter()


@router.get("/")
async def root(request: Request):
    return JSONResponse({"status": "ok"})


@router.get("/catalog")
async def catalog(request: Request, filter: str = "Top", page: int = 0):
    user_data = auth.get_user_data(request)
    try:
        if filter == "Recommendations":
            book_lists = BookList(filter, user_data.get("preferred_username"))
            book_list, pages = book_lists.get_recommendation_book_list(page)
        else:
            book_lists = BookList(filter)
            book_list, pages = book_lists.get_book_list(page)
    except Exception:
        book_list, pages = [], 0

    return JSONResponse({"books": [], "pages": pages})


@router.get("/personal")
async def personal(request: Request):
    return JSONResponse({"status": "ok"})


@router.post("/book")
async def book_post(request: Request, id: int, page: int = 0, comment: str = Form(...)):
    user_data = auth.get_user_data(request)

    try:
        Book(id)
    except ObjectNotFound:
        return JSONResponse({"error": "not found"}, status_code=404)

    if user_data:
        username = user_data["preferred_username"]
        Message(username, id, comment).set_message()

    return JSONResponse({"status": "ok"})


@router.get("/book")
async def book(request: Request, id: int, page: int = 0):
    user_data = auth.get_user_data(request)

    try:
        Book(id)
    except ObjectNotFound:
        return JSONResponse({"error": "not found"}, status_code=404)

    return JSONResponse({"status": "ok"})


@router.post("/search")
async def search(request: Request, search_string: str = Form(...)):
    import subprocess

    levenshtein_cwd = os.environ.get(
        "SEARCH_LEVENSHTEIN_DIR", "src/scripts/searching_mechanism"
    )
    result = subprocess.Popen(
        ["./levenshtein_length"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=levenshtein_cwd,
    )
    output_data, stderr_data = result.communicate(input=search_string + "\n")
    output_lines = output_data.splitlines()

    cleaned_lines = [
        line.strip()
        for line in output_lines
        if line.strip() and not line.startswith("----")
    ]

    search_instance = Search(cleaned_lines)
    books = search_instance.get_search_result()

    return JSONResponse({"books": [], "query": search_string})


@router.get("/signin")
async def signin_get(request: Request):
    return JSONResponse({"status": "ok"})


@router.get("/lost")
async def lost(request: Request):
    return JSONResponse({"error": "not found"}, status_code=404)


@router.get("/registration")
async def register(request: Request):
    return JSONResponse({"status": "ok"})


@router.post("/registration")
async def register_post(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
):
    username = username.lower()
    if password != password_confirm:
        return JSONResponse({"error": "passwords don't match"}, status_code=400)

    try:
        auth.create_user(username, password, email)
    except UsernameNotUnique:
        return JSONResponse({"error": "username already taken"}, status_code=409)

    return await signin_post(request, username, password)


@router.post("/signin")
async def signin_post(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    username = username.lower()
    try:
        access, refresh = auth.authenticate(username=username, password=password)
    except BadCredentials:
        return JSONResponse({"error": "wrong password or username"}, status_code=401)

    response = RedirectResponse("/personal", status_code=303)
    response.set_cookie("access", access)
    response.set_cookie("refresh", refresh)
    return response


@router.get("/logout")
async def logout(request: Request):
    response = RedirectResponse("/", status_code=303)
    refresh = request.cookies.get("refresh")
    if not refresh:
        return response
    auth.logout(refresh)
    response.delete_cookie("refresh")
    response.delete_cookie("access")
    return response
