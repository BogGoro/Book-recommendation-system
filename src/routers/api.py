import os
import subprocess

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from src.scripts import auth
from src.scripts.book import Book
from src.scripts.book_list import BookList
from src.scripts.book_messages import BookMessages
from src.scripts.book_stats import BookStats
from src.scripts.exceptions import BadCredentials, ObjectNotFound, UsernameNotUnique
from src.scripts.message import Message
from src.scripts.score import Score
from src.scripts.search import Search
from src.scripts.status import Status
from src.scripts.user_list import UserList
from src.scripts.user_stats import UserStats
from src.constants import TOP_LIST

router = APIRouter(prefix="/api")


def _serialize_book(book: Book) -> dict:
    return {
        "id": book.id,
        "title": book.title,
        "author": book.author,
        "cover": book.cover,
        "description": book.description,
    }


def _serialize_book_stats(stats: BookStats) -> dict:
    return {
        "statuses": stats.statuses,
        "scores": stats.scores,
    }


# ── Auth ──────────────────────────────────────────────────────────────────────

@router.get("/me")
async def me(request: Request):
    user_data = auth.get_user_data(request)
    if not user_data:
        return Response(status_code=401)
    return user_data


class SignInBody(BaseModel):
    username: str
    password: str


@router.post("/signin")
async def signin(body: SignInBody):
    username = body.username.lower()
    try:
        access, refresh = auth.authenticate(username=username, password=body.password)
    except BadCredentials:
        return Response(status_code=401)

    response = JSONResponse({"preferred_username": username})
    response.set_cookie("access", access, httponly=True, samesite="lax")
    response.set_cookie("refresh", refresh, httponly=True, samesite="lax")
    return response


class RegistrationBody(BaseModel):
    username: str
    email: str
    password: str


@router.post("/registration")
async def registration(body: RegistrationBody):
    username = body.username.lower()
    try:
        auth.create_user(username, body.password, body.email)
    except UsernameNotUnique:
        return Response(status_code=409)

    try:
        access, refresh = auth.authenticate(username=username, password=body.password)
    except BadCredentials:
        return Response(status_code=500)

    response = JSONResponse({"preferred_username": username})
    response.set_cookie("access", access, httponly=True, samesite="lax")
    response.set_cookie("refresh", refresh, httponly=True, samesite="lax")
    return response


@router.post("/logout")
async def logout(request: Request):
    refresh = request.cookies.get("refresh")
    if refresh:
        auth.logout(refresh)
    response = Response(status_code=200)
    response.delete_cookie("access")
    response.delete_cookie("refresh")
    return response


# ── Catalog ───────────────────────────────────────────────────────────────────

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

    return {"books": [_serialize_book(b) for b in book_list], "pages": pages}


# ── Book ──────────────────────────────────────────────────────────────────────

@router.get("/book")
async def book(request: Request, id: int):
    user_data = auth.get_user_data(request)

    try:
        b = Book(id)
    except ObjectNotFound:
        return Response(status_code=404)

    book_stats = BookStats(id)

    if not user_data:
        return {
            "book": _serialize_book(b),
            "status": None,
            "score": None,
            "book_stats": _serialize_book_stats(book_stats),
            "comments_allowed": False,
        }

    try:
        status = Status(user_data["preferred_username"], id).status or "untracked"
    except ObjectNotFound:
        status = "untracked"
    try:
        score = Score(user_data["preferred_username"], id).score or 0
    except ObjectNotFound:
        score = 0

    return {
        "book": _serialize_book(b),
        "status": status,
        "score": score,
        "book_stats": _serialize_book_stats(book_stats),
        "comments_allowed": True,
    }


@router.get("/book/comments")
async def book_comments(id: int, page: int = 0):
    try:
        Book(id)
    except ObjectNotFound:
        return Response(status_code=404)

    messages = BookMessages(id)
    raw = messages.get_book_comments(page)
    pages = messages.get_pages_count()

    comments = [{"author": row[0], "text": row[1]} for row in raw]
    return {"comments": comments, "pages": pages}


class CommentBody(BaseModel):
    id: int
    comment: str
    page: int = 0


@router.post("/book/comment")
async def post_comment(request: Request, body: CommentBody):
    user_data = auth.get_user_data(request)
    if not user_data:
        return Response(status_code=401)

    try:
        Book(body.id)
    except ObjectNotFound:
        return Response(status_code=404)

    Message(user_data["preferred_username"], body.id, body.comment).set_message()
    return Response(status_code=200)


# ── Search ────────────────────────────────────────────────────────────────────

class SearchBody(BaseModel):
    search_string: str


@router.post("/search")
async def search(body: SearchBody):
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
    output_data, _ = result.communicate(input=body.search_string + "\n")
    cleaned_lines = [
        line.strip()
        for line in output_data.splitlines()
        if line.strip() and not line.startswith("----")
    ]

    books = Search(cleaned_lines).get_search_result()
    return {"books": [_serialize_book(b) for b in books], "query": body.search_string}


# ── Personal ──────────────────────────────────────────────────────────────────

@router.get("/personal")
async def personal(request: Request):
    user_data = auth.get_user_data(request)
    if not user_data:
        return Response(status_code=401)

    username = user_data["preferred_username"]

    def serialize_list(entries):
        result = []
        for book, score_obj in entries:
            result.append({
                "book": _serialize_book(book),
                "score": score_obj.score,
            })
        return result

    try:
        user_lists = UserList(username)
        completed = serialize_list(user_lists.get_completed_list())
        reading = serialize_list(user_lists.get_reading_list())
        planned = serialize_list(user_lists.get_planned_list())
    except ObjectNotFound:
        completed, reading, planned = [], [], []

    try:
        stats = UserStats(username)
        user_stats = {"scores": stats.scores}
    except ObjectNotFound:
        user_stats = {"scores": [0, 0, 0, 0, 0]}

    return {
        "completed": completed,
        "reading": reading,
        "planned": planned,
        "user_stats": user_stats,
    }
