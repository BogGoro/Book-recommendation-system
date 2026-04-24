import { BrowserRouter, Routes, Route, useLocation } from 'react-router-dom'
import { AuthProvider } from './context/AuthContext'
import Navbar from './components/Navbar'
import Notification from './components/Notification'
import Home from './pages/Home'
import Catalog from './pages/Catalog'
import BookInfo from './pages/BookInfo'
import Search from './pages/Search'
import SignIn from './pages/SignIn'
import Registration from './pages/Registration'
import Personal from './pages/Personal'
import NotFound from './pages/NotFound'

const NO_NAVBAR = ['/signin', '/registration']

function Layout() {
  const { pathname } = useLocation()
  const showNavbar = !NO_NAVBAR.includes(pathname)

  return (
    <>
      <img className="image-gradient" src="/gradient.png" alt="" />
      <div className="layer-blur" />
      <Notification />
      <div className="container">
        {showNavbar && <Navbar />}
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/catalog" element={<Catalog />} />
          <Route path="/book" element={<BookInfo />} />
          <Route path="/search" element={<Search />} />
          <Route path="/signin" element={<SignIn />} />
          <Route path="/registration" element={<Registration />} />
          <Route path="/personal" element={<Personal />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </div>
    </>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Layout />
      </AuthProvider>
    </BrowserRouter>
  )
}
