import client from './client'
import type { ReadingStatus } from '../types'

export async function setScore(bookId: number, score: number): Promise<void> {
  const form = new FormData()
  form.append('book_id', String(bookId))
  form.append('score', String(score))
  await client.post('/feedback', form)
}

export async function setStatus(bookId: number, status: ReadingStatus): Promise<void> {
  const form = new FormData()
  form.append('book_id', String(bookId))
  form.append('status', status)
  await client.post('/feedback', form)
}
