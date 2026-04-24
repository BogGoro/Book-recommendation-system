import { Link } from 'react-router-dom'

export default function NotFound() {
  return (
    <main>
      <div className="error-page">
        <div className="error-illustration">
          <span className="digit jump">4</span>
          <span className="digit jump delay">0</span>
          <span className="digit jump">4</span>
        </div>
        <h2>Page not found</h2>
        <p style={{ color: 'var(--grey)', marginTop: '1rem' }}>
          The page you're looking for doesn't exist.
        </p>
        <div style={{ marginTop: '2rem' }}>
          <Link to="/" className="btn-get-started" style={{ color: 'inherit' }}>
            Go Home
          </Link>
        </div>
      </div>
    </main>
  )
}
