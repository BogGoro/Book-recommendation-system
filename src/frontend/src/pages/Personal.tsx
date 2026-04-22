import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { getPersonal } from '../api/books'
import type { UserBookEntry, UserStats } from '../types'
import BookCard from '../components/BookCard'
import { useAuth } from '../context/AuthContext'

const LISTS = ['completed', 'reading', 'planned'] as const
const LIST_LABELS = { completed: 'Already read', reading: 'In progress', planned: 'Planned' }

export default function Personal() {
  const { user, loading: authLoading } = useAuth()
  const navigate = useNavigate()

  const [lists, setLists] = useState<{
    completed: UserBookEntry[]
    reading: UserBookEntry[]
    planned: UserBookEntry[]
  }>({ completed: [], reading: [], planned: [] })
  const [userStats, setUserStats] = useState<UserStats | null>(null)
  const [activeList, setActiveList] = useState<typeof LISTS[number]>('completed')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!authLoading && !user) {
      navigate('/signin')
    }
  }, [user, authLoading, navigate])

  useEffect(() => {
    if (!user) return
    getPersonal()
      .then((data) => {
        setLists({ completed: data.completed, reading: data.reading, planned: data.planned })
        setUserStats(data.user_stats)
      })
      .finally(() => setLoading(false))
  }, [user])

  if (authLoading || loading) {
    return <main className="book-catalog"><p>Loading...</p></main>
  }

  const containerOffset = { completed: 0, reading: 100, planned: 200 }

  return (
    <main className="book-catalog">
      {userStats && (
        <div className="side-nav-rating">
          <div className="progress-bars">
            <h3 className="progress-title">Given rating</h3>
            {[5, 4, 3, 2, 1].map((star, i) => (
              <div key={star} className="progress-item">
                <span className="progress-label">{star}</span>
                <div className="progress-bar">
                  <div
                    className="progress-fill"
                    style={{ width: `${userStats.scores[4 - i]}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="selector">
        {LISTS.map((list) => (
          <div
            key={list}
            id={`status-${list}`}
            className={`select-btn ${activeList === list ? 'active' : ''}`}
            onClick={() => setActiveList(list)}
          >
            {LIST_LABELS[list]}
          </div>
        ))}
      </div>

      <div className="book-lists">
        <div
          id="book-lists-container"
          style={{ marginLeft: `-${containerOffset[activeList]}%` }}
        >
          {LISTS.map((list) => (
            <div key={list} className="book-grid">
              {lists[list].map((entry, i) => (
                <div key={i}>
                  <BookCard book={entry.book} score={entry.score} />
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </main>
  )
}
