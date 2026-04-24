import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { getBook, getComments, postComment } from '../api/books'
import { setScore, setStatus } from '../api/feedback'
import type { Book, BookStats, Comment, ReadingStatus } from '../types'
import StarRating from '../components/StarRating'
import Pagination from '../components/Pagination'
import { showNotification } from '../components/Notification'

const STATUS_LABELS: { value: ReadingStatus; label: string }[] = [
  { value: 'completed', label: 'Already read' },
  { value: 'reading', label: 'In progress' },
  { value: 'planned', label: 'Planned' },
  { value: 'untracked', label: 'Untracked' },
]

export default function BookInfo() {
  const [searchParams, setSearchParams] = useSearchParams()
  const id = parseInt(searchParams.get('id') || '0', 10)
  const commentPage = parseInt(searchParams.get('page') || '0', 10)

  const [book, setBook] = useState<Book | null>(null)
  const [bookStats, setBookStats] = useState<BookStats | null>(null)
  const [score, setScoreState] = useState<number>(0)
  const [status, setStatusState] = useState<ReadingStatus>('untracked')
  const [commentsAllowed, setCommentsAllowed] = useState(false)
  const [comments, setComments] = useState<Comment[]>([])
  const [commentPages, setCommentPages] = useState(0)
  const [commentText, setCommentText] = useState('')
  const [notFound, setNotFound] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!id) return
    setLoading(true)
    getBook(id)
      .then((data) => {
        setBook(data.book)
        setBookStats(data.book_stats)
        setScoreState(data.score ?? 0)
        setStatusState((data.status as ReadingStatus) ?? 'untracked')
        setCommentsAllowed(data.comments_allowed)
      })
      .catch(() => setNotFound(true))
      .finally(() => setLoading(false))
  }, [id])

  useEffect(() => {
    if (!id) return
    getComments(id, commentPage).then((data) => {
      setComments(data.comments)
      setCommentPages(data.pages)
    })
  }, [id, commentPage])

  async function handleScoreChange(newScore: number) {
    setScoreState(newScore)
    await setScore(id, newScore)
    showNotification('Rating saved')
  }

  async function handleRemoveMark() {
    setScoreState(0)
    await setScore(id, 0)
    showNotification('Rating removed')
  }

  async function handleStatusChange(s: ReadingStatus) {
    setStatusState(s)
    await setStatus(id, s)
    showNotification('Status saved')
  }

  async function handleCommentSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!commentText.trim()) return
    await postComment(id, commentText, commentPage)
    setCommentText('')
    getComments(id, commentPage).then((data) => {
      setComments(data.comments)
      setCommentPages(data.pages)
    })
  }

  if (loading) return <main className="book-catalog"><p>Loading...</p></main>
  if (notFound || !book) return <NotFoundInline />

  return (
    <main className="book-catalog">
      <div className="book-header">
        <div className="book-info">
          <div className="book-cover anim-border">
            <img src={book.cover} alt={book.title} />
            <span />
            <span />
          </div>

          <div className="book-details">
            <h2 className="book-title">{book.title}</h2>
            <p className="book-author">{book.author}</p>

            {commentsAllowed && (
              <>
                <StarRating score={score} onChange={handleScoreChange} />
                {score > 0 && (
                  <div className="tag-box" style={{ marginTop: '8px' }}>
                    <span className="tag" onClick={handleRemoveMark}>
                      Remove mark
                    </span>
                  </div>
                )}
              </>
            )}

            <p className="book-description" style={{ marginTop: '1rem' }}>
              {book.description}
            </p>

            {commentsAllowed && (
              <div className="status-save-wrapper" style={{ marginTop: '1rem' }}>
                <div className="selector">
                  {STATUS_LABELS.map(({ value, label }) => (
                    <div
                      key={value}
                      className={`select-btn ${status === value ? 'active' : ''}`}
                      onClick={() => handleStatusChange(value)}
                    >
                      {label}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {bookStats && (
        <div className="side-nav-prog">
          <div className="progress-bars">
            <h3 className="progress-title">On user lists:</h3>
            {[
              { label: 'Already read', value: bookStats.statuses[0] },
              { label: 'In progress', value: bookStats.statuses[1] },
              { label: 'Planned', value: bookStats.statuses[2] },
            ].map(({ label, value }) => (
              <div key={label} className="progress-item">
                <span className="progress-label">{label}</span>
                <div className="progress-bar">
                  <div className="progress-fill" style={{ width: `${value}%` }} />
                </div>
              </div>
            ))}
            <p>Average score: {bookStats.scores[5]}</p>
            {[5, 4, 3, 2, 1].map((star, i) => (
              <div key={star} className="progress-item small">
                <span className="progress-label">{star}☆</span>
                <div className="progress-bar">
                  <div className="progress-fill" style={{ width: `${bookStats.scores[4 - i]}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="comments-section">
        <h3>Comments</h3>
        {commentsAllowed && (
          <form onSubmit={handleCommentSubmit} className="comment-form">
            <textarea
              value={commentText}
              onChange={(e) => setCommentText(e.target.value)}
              placeholder="Leave a comment..."
              rows={4}
              required
            />
            <button type="submit">Post Comment</button>
          </form>
        )}

        <div className="comment-list">
          {comments.map((c, i) => (
            <div key={i} className="comment">
              <p className="comment-author">
                <strong>{c.author}</strong>
              </p>
              <p className="comment-text">{c.text}</p>
            </div>
          ))}
        </div>

        <Pagination
          currentPage={commentPage}
          totalPages={commentPages}
          onPageChange={(p) => setSearchParams({ id: String(id), page: String(p) })}
        />
      </div>
    </main>
  )
}

function NotFoundInline() {
  return (
    <main className="book-catalog">
      <div className="error-page">
        <h2>Book not found</h2>
      </div>
    </main>
  )
}
