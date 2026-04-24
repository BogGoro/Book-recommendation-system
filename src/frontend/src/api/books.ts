import client from './client'
import type { Book, BookStats, Comment, UserBookEntry, UserStats } from '../types'

export interface CatalogResponse {
  books: Book[]
  pages: number
}

export async function getCatalog(filter: string, page: number): Promise<CatalogResponse> {
  const res = await client.get<CatalogResponse>('/catalog', { params: { filter, page } })
  return res.data
}

export interface BookResponse {
  book: Book
  status: string | null
  score: number | null
  book_stats: BookStats
  comments_allowed: boolean
}

export async function getBook(id: number): Promise<BookResponse> {
  const res = await client.get<BookResponse>('/book', { params: { id } })
  return res.data
}

export interface CommentsResponse {
  comments: Comment[]
  pages: number
}

export async function getComments(bookId: number, page: number): Promise<CommentsResponse> {
  const res = await client.get<CommentsResponse>('/book/comments', {
    params: { id: bookId, page },
  })
  return res.data
}

export async function postComment(bookId: number, comment: string, page: number): Promise<void> {
  await client.post('/book/comment', { id: bookId, comment, page })
}

export interface SearchResponse {
  books: Book[]
  query: string
}

export async function searchBooks(query: string): Promise<SearchResponse> {
  const res = await client.post<SearchResponse>('/search', { search_string: query })
  return res.data
}

export interface PersonalResponse {
  completed: UserBookEntry[]
  reading: UserBookEntry[]
  planned: UserBookEntry[]
  user_stats: UserStats
}

export async function getPersonal(): Promise<PersonalResponse> {
  const res = await client.get<PersonalResponse>('/personal')
  return res.data
}
