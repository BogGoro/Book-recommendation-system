import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import Pagination from '../components/Pagination'

describe('Pagination', () => {
  it('не рендерится при totalPages <= 1', () => {
    const { container } = render(
      <Pagination currentPage={0} totalPages={1} onPageChange={vi.fn()} />
    )
    expect(container.firstChild).toBeNull()
  })

  it('рендерит кнопки First и Last', () => {
    render(<Pagination currentPage={2} totalPages={10} onPageChange={vi.fn()} />)
    expect(screen.getByText('First')).toBeInTheDocument()
    expect(screen.getByText('Last')).toBeInTheDocument()
  })

  it('кнопка First вызывает onPageChange(0)', () => {
    const onPageChange = vi.fn()
    render(<Pagination currentPage={5} totalPages={10} onPageChange={onPageChange} />)
    fireEvent.click(screen.getByText('First'))
    expect(onPageChange).toHaveBeenCalledWith(0)
  })

  it('кнопка Last вызывает onPageChange(totalPages - 1)', () => {
    const onPageChange = vi.fn()
    render(<Pagination currentPage={0} totalPages={10} onPageChange={onPageChange} />)
    fireEvent.click(screen.getByText('Last'))
    expect(onPageChange).toHaveBeenCalledWith(9)
  })

  it('показывает кнопку < если currentPage > 0', () => {
    render(<Pagination currentPage={3} totalPages={10} onPageChange={vi.fn()} />)
    expect(screen.getByText('<')).toBeInTheDocument()
  })

  it('не показывает кнопку < на первой странице', () => {
    render(<Pagination currentPage={0} totalPages={10} onPageChange={vi.fn()} />)
    expect(screen.queryByText('<')).toBeNull()
  })

  it('показывает кнопку > если есть следующая страница', () => {
    render(<Pagination currentPage={0} totalPages={10} onPageChange={vi.fn()} />)
    expect(screen.getByText('>')).toBeInTheDocument()
  })

  it('не показывает кнопку > на последней странице', () => {
    render(<Pagination currentPage={9} totalPages={10} onPageChange={vi.fn()} />)
    expect(screen.queryByText('>')).toBeNull()
  })

  it('клик по номеру страницы вызывает onPageChange с нужным индексом', () => {
    const onPageChange = vi.fn()
    render(<Pagination currentPage={0} totalPages={5} onPageChange={onPageChange} />)
    fireEvent.click(screen.getByText('3'))
    expect(onPageChange).toHaveBeenCalledWith(2)
  })
})
