import { useNavigate } from 'react-router-dom'
import type { Book } from '../types'

interface Props {
  book: Book
  score?: number | null
}

export default function BookCard({ book, score }: Props) {
  const navigate = useNavigate()

  return (
    <div className="book-card" onClick={() => navigate(`/book?id=${book.id}`)}>
      <img src={book.cover} alt={book.title} />
      <h3 className="book-title">{book.title}</h3>
      <p className="book-author">{book.author}</p>
      {score != null && score > 0 && (
        <p className="user-rating">{'☆'.repeat(score)}</p>
      )}
    </div>
  )
}
