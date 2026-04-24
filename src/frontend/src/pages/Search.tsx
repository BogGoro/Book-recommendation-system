import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { searchBooks } from '../api/books'
import type { Book } from '../types'
import BookCard from '../components/BookCard'

export default function Search() {
  const [searchParams] = useSearchParams()
  const query = searchParams.get('q') || ''
  const [books, setBooks] = useState<Book[]>([])
  const [loading, setLoading] = useState(false)
  const [searched, setSearched] = useState(false)

  useEffect(() => {
    if (!query) return
    setLoading(true)
    setSearched(true)
    searchBooks(query)
      .then((data) => setBooks(data.books))
      .catch(() => setBooks([]))
      .finally(() => setLoading(false))
  }, [query])

  return (
    <main className="book-catalog">
      <h2 className="section-title">
        {query ? `Search results for "${query}"` : 'Search'}
      </h2>

      {loading && <p style={{ marginLeft: '3rem' }}>Searching...</p>}

      {!loading && searched && books.length === 0 && (
        <p style={{ marginLeft: '3rem' }}>No results found.</p>
      )}

      <div className="book-grid">
        {books.map((book) => (
          <BookCard key={book.id} book={book} />
        ))}
      </div>
    </main>
  )
}
