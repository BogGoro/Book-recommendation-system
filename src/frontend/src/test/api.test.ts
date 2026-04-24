import { describe, it, expect, vi, beforeEach } from 'vitest'
import axios from 'axios'

vi.mock('axios')
const mockedAxios = vi.mocked(axios, true)

vi.mock('../api/client', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}))

import client from '../api/client'
import { getMe, signIn, register, logout } from '../api/auth'
import { getCatalog, getBook, getComments, searchBooks, getPersonal } from '../api/books'
import { setScore, setStatus } from '../api/feedback'

const mockedClient = vi.mocked(client)

beforeEach(() => {
  vi.clearAllMocks()
})

// ── Auth ──────────────────────────────────────────────────────────────────────

describe('auth API', () => {
  it('getMe возвращает данные пользователя при 200', async () => {
    mockedClient.get.mockResolvedValueOnce({ data: { preferred_username: 'alice' } })
    const result = await getMe()
    expect(result).toEqual({ preferred_username: 'alice' })
    expect(mockedClient.get).toHaveBeenCalledWith('/me')
  })

  it('getMe возвращает null при ошибке', async () => {
    mockedClient.get.mockRejectedValueOnce(new Error('401'))
    const result = await getMe()
    expect(result).toBeNull()
  })

  it('signIn отправляет username и password', async () => {
    mockedClient.post.mockResolvedValueOnce({ data: { preferred_username: 'alice' } })
    await signIn('alice', 'secret')
    expect(mockedClient.post).toHaveBeenCalledWith('/signin', { username: 'alice', password: 'secret' })
  })

  it('register отправляет username, email и password', async () => {
    mockedClient.post.mockResolvedValueOnce({ data: { preferred_username: 'alice' } })
    await register('alice', 'alice@mail.com', 'secret')
    expect(mockedClient.post).toHaveBeenCalledWith('/registration', {
      username: 'alice',
      email: 'alice@mail.com',
      password: 'secret',
    })
  })

  it('logout вызывает POST /logout', async () => {
    mockedClient.post.mockResolvedValueOnce({ data: {} })
    await logout()
    expect(mockedClient.post).toHaveBeenCalledWith('/logout')
  })
})

// ── Books ─────────────────────────────────────────────────────────────────────

describe('books API', () => {
  it('getCatalog передаёт filter и page', async () => {
    mockedClient.get.mockResolvedValueOnce({ data: { books: [], pages: 0 } })
    await getCatalog('Top', 2)
    expect(mockedClient.get).toHaveBeenCalledWith('/catalog', { params: { filter: 'Top', page: 2 } })
  })

  it('getCatalog возвращает books и pages', async () => {
    const mockData = { books: [{ id: 1, title: 'Книга', author: 'Автор', cover: '' }], pages: 3 }
    mockedClient.get.mockResolvedValueOnce({ data: mockData })
    const result = await getCatalog('Top', 0)
    expect(result.pages).toBe(3)
    expect(result.books).toHaveLength(1)
  })

  it('getBook передаёт id', async () => {
    mockedClient.get.mockResolvedValueOnce({
      data: { book: {}, status: 'reading', score: 0, book_stats: {}, comments_allowed: true },
    })
    await getBook(42)
    expect(mockedClient.get).toHaveBeenCalledWith('/book', { params: { id: 42 } })
  })

  it('getComments передаёт bookId и page', async () => {
    mockedClient.get.mockResolvedValueOnce({ data: { comments: [], pages: 0 } })
    await getComments(5, 1)
    expect(mockedClient.get).toHaveBeenCalledWith('/book/comments', { params: { id: 5, page: 1 } })
  })

  it('searchBooks отправляет search_string', async () => {
    mockedClient.post.mockResolvedValueOnce({ data: { books: [], query: 'Булгаков' } })
    await searchBooks('Булгаков')
    expect(mockedClient.post).toHaveBeenCalledWith('/search', { search_string: 'Булгаков' })
  })

  it('getPersonal возвращает completed, reading, planned, user_stats', async () => {
    const mockData = { completed: [], reading: [], planned: [], user_stats: { scores: [0, 0, 0, 0, 0] } }
    mockedClient.get.mockResolvedValueOnce({ data: mockData })
    const result = await getPersonal()
    expect(result).toEqual(mockData)
  })
})

// ── Feedback ──────────────────────────────────────────────────────────────────

describe('feedback API', () => {
  it('setScore отправляет FormData с book_id и score', async () => {
    mockedClient.post.mockResolvedValueOnce({ data: 'OK' })
    await setScore(1, 4)
    const [url, form] = mockedClient.post.mock.calls[0]
    expect(url).toBe('/feedback')
    expect(form.get('book_id')).toBe('1')
    expect(form.get('score')).toBe('4')
  })

  it('setStatus отправляет FormData с book_id и status', async () => {
    mockedClient.post.mockResolvedValueOnce({ data: 'OK' })
    await setStatus(1, 'reading')
    const [url, form] = mockedClient.post.mock.calls[0]
    expect(url).toBe('/feedback')
    expect(form.get('book_id')).toBe('1')
    expect(form.get('status')).toBe('reading')
  })
})
