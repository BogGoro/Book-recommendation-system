import { Link, useNavigate } from 'react-router-dom'
import { useState } from 'react'
import { useAuth } from '../context/AuthContext'

export default function Navbar() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  const [query, setQuery] = useState('')

  function handleSearch(e: React.FormEvent) {
    e.preventDefault()
    if (query.trim()) {
      navigate(`/search?q=${encodeURIComponent(query.trim())}`)
      setQuery('')
    }
  }

  async function handleLogout() {
    await logout()
    navigate('/')
  }

  return (
    <header>
      <h1 className="logo">
        <Link to="/" style={{ textDecoration: 'none', color: 'inherit' }}>
          Recommendation System
        </Link>
      </h1>

      <form onSubmit={handleSearch} className="search-box" style={{ display: 'flex', gap: '8px' }}>
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search..."
          className="search-input"
        />
        <button type="submit" className="search-button">
          Search
        </button>
      </form>

      <div className="user-box">
        {user ? (
          <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
            <img src="/avatar.svg" alt="User Avatar" width="40" height="40" />
            <span className="username">
              <Link to="/personal" style={{ textDecoration: 'none', color: 'inherit' }}>
                {user.preferred_username}
              </Link>
            </span>
            <button
              onClick={handleLogout}
              style={{
                background: 'transparent',
                border: 'none',
                fontSize: '22pt',
                cursor: 'pointer',
                color: 'white',
                padding: 0,
              }}
              title="Log out"
            >
              <i className="bi bi-box-arrow-right" />
            </button>
          </div>
        ) : (
          <div style={{ display: 'flex', gap: '10px' }}>
            <Link to="/signin" className="btn-signing" style={{ textDecoration: 'none', color: 'black' }}>
              SIGN IN
            </Link>
            <Link to="/registration" className="btn-signing" style={{ textDecoration: 'none', color: 'black' }}>
              SIGN UP
            </Link>
          </div>
        )}
      </div>
    </header>
  )
}
