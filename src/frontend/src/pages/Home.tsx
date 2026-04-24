import { Link } from 'react-router-dom'

export default function Home() {
  return (
    <main>
      <div className="content">
        <div className="tag-box">
          <Link to="/catalog" className="tag">
            Browse the catalog →
          </Link>
        </div>
        <h1>Find your next favourite book</h1>
        <p className="description">
          Personalised recommendations based on your reading history and preferences.
        </p>
        <div className="buttons">
          <Link to="/catalog" className="btn-get-started" style={{ color: 'inherit' }}>
            Get Started
          </Link>
          <Link to="/registration" className="btn-signing-main">
            Sign Up
          </Link>
        </div>
      </div>

      <spline-viewer
        className="bookshelve"
        url="https://prod.spline.design/6sSB4NA7WSJtxw3F/scene.splinecode"
      />
    </main>
  )
}

