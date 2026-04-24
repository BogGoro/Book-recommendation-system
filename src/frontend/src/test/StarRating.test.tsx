import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import StarRating from '../components/StarRating'

describe('StarRating', () => {
  it('отображает правильное количество заполненных звёзд', () => {
    render(<StarRating score={3} onChange={vi.fn()} />)
    const stars = screen.getAllByText(/[★☆]/)
    const filled = stars.filter((s) => s.textContent === '★')
    const empty = stars.filter((s) => s.textContent === '☆')
    expect(filled).toHaveLength(3)
    expect(empty).toHaveLength(2)
  })

  it('отображает счёт в формате X/5', () => {
    render(<StarRating score={4} onChange={vi.fn()} />)
    expect(screen.getByText('4/5')).toBeInTheDocument()
  })

  it('вызывает onChange с правильным значением при клике', () => {
    const onChange = vi.fn()
    render(<StarRating score={0} onChange={onChange} />)
    const stars = screen.getAllByText('☆')
    fireEvent.click(stars[2])
    expect(onChange).toHaveBeenCalledWith(3)
  })

  it('не вызывает onChange при клике в режиме disabled', () => {
    const onChange = vi.fn()
    render(<StarRating score={0} onChange={onChange} disabled />)
    const stars = screen.getAllByText('☆')
    fireEvent.click(stars[0])
    expect(onChange).not.toHaveBeenCalled()
  })

  it('показывает 0/5 при score=0', () => {
    render(<StarRating score={0} onChange={vi.fn()} />)
    expect(screen.getByText('0/5')).toBeInTheDocument()
    const stars = screen.getAllByText('☆')
    expect(stars).toHaveLength(5)
  })
})
