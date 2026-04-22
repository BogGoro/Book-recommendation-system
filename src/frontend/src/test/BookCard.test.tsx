import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import BookCard from '../components/BookCard'

const mockNavigate = vi.fn()
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return { ...actual, useNavigate: () => mockNavigate }
})

const book = {
  id: 42,
  title: 'Мастер и Маргарита',
  author: 'Михаил Булгаков',
  cover: 'https://example.com/cover.jpg',
}

describe('BookCard', () => {
  it('отображает название книги', () => {
    render(<MemoryRouter><BookCard book={book} /></MemoryRouter>)
    expect(screen.getByText('Мастер и Маргарита')).toBeInTheDocument()
  })

  it('отображает автора', () => {
    render(<MemoryRouter><BookCard book={book} /></MemoryRouter>)
    expect(screen.getByText('Михаил Булгаков')).toBeInTheDocument()
  })

  it('отображает обложку с правильным alt', () => {
    render(<MemoryRouter><BookCard book={book} /></MemoryRouter>)
    const img = screen.getByAltText('Мастер и Маргарита') as HTMLImageElement
    expect(img.src).toBe('https://example.com/cover.jpg')
  })

  it('при клике переходит на /book?id=42', () => {
    render(<MemoryRouter><BookCard book={book} /></MemoryRouter>)
    fireEvent.click(screen.getByText('Мастер и Маргарита'))
    expect(mockNavigate).toHaveBeenCalledWith('/book?id=42')
  })

  it('не показывает рейтинг при score=0', () => {
    render(<MemoryRouter><BookCard book={book} score={0} /></MemoryRouter>)
    expect(screen.queryByText(/☆+/)).toBeNull()
  })

  it('не показывает рейтинг если score не передан', () => {
    render(<MemoryRouter><BookCard book={book} /></MemoryRouter>)
    expect(screen.queryByText(/☆+/)).toBeNull()
  })

  it('показывает рейтинг звёздами при score > 0', () => {
    render(<MemoryRouter><BookCard book={book} score={3} /></MemoryRouter>)
    expect(screen.getByText('☆☆☆')).toBeInTheDocument()
  })
})
