import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { getCatalog } from '../api/books'
import type { Book } from '../types'
import BookCard from '../components/BookCard'
import Pagination from '../components/Pagination'
import { useAuth } from '../context/AuthContext'

const FILTERS = ['Top', 'WeeklyTop', 'Recommendations']

export default function Catalog() {
  const [searchParams, setSearchParams] = useSearchParams()
  const filter = searchParams.get('filter') || 'Top'
  const page = parseInt(searchParams.get('page') || '0', 10)

  const { user } = useAuth()
  const [books, setBooks] = useState<Book[]>([])
  const [totalPages, setTotalPages] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    getCatalog(filter, page)
      .then((data) => {
        setBooks(data.books)
        setTotalPages(data.pages)
      })
      .catch(() => {
        setBooks([])
        setTotalPages(0)
      })
      .finally(() => setLoading(false))
  }, [filter, page])

  function setFilter(f: string) {
    setSearchParams({ filter: f, page: '0' })
  }

  function setPage(p: number) {
    setSearchParams({ filter, page: String(p) })
  }

  return (
    <main className="book-catalog">
      <h2 id="section-title" className="section-title">
        {filter}
      </h2>

      {loading ? (
        <p style={{ marginLeft: '3rem' }}>Loading...</p>
      ) : (
        <>
          <div className="book-grid">
            {books.map((book) => (
              <BookCard key={book.id} book={book} />
            ))}
          </div>
          <Pagination currentPage={page} totalPages={totalPages} onPageChange={setPage} />
        </>
      )}

      <div className="side-nav">
        <div className="catalog-sidebar">
          <h3 className="filter-title">Filter by:</h3>
          <div className="filter-options">
            {FILTERS.filter((f) => f !== 'Recommendations' || !!user).map((f) => (
              <div key={f} className="radio-option">
                <input
                  type="radio"
                  id={`filter-${f}`}
                  name="filter"
                  value={f}
                  checked={filter === f}
                  onChange={() => setFilter(f)}
                />
                <label htmlFor={`filter-${f}`}>
                  {f === 'WeeklyTop' ? 'Weekly Top' : f}
                </label>
              </div>
            ))}
          </div>
        </div>
      </div>
    </main>
  )
}
