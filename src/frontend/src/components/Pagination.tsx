interface Props {
  currentPage: number
  totalPages: number
  onPageChange: (page: number) => void
}

export default function Pagination({ currentPage, totalPages, onPageChange }: Props) {
  if (totalPages <= 1) return null

  const start = Math.max(0, currentPage - 2)
  const end = Math.min(totalPages, currentPage + 3)
  const pages = Array.from({ length: end - start }, (_, i) => start + i)

  return (
    <div className="pagination-wrapper" style={{ maxWidth: '960px' }}>
      <button className="pagination-link" onClick={() => onPageChange(0)}>
        First
      </button>
      {currentPage > 0 && (
        <button className="pagination-link" onClick={() => onPageChange(currentPage - 1)}>
          &lt;
        </button>
      )}
      {pages.map((p) => (
        <button
          key={p}
          className="pagination-link"
          onClick={() => onPageChange(p)}
          style={p === currentPage ? { color: 'white' } : undefined}
        >
          {p + 1}
        </button>
      ))}
      {currentPage + 1 < totalPages && (
        <button className="pagination-link" onClick={() => onPageChange(currentPage + 1)}>
          &gt;
        </button>
      )}
      <button className="pagination-link" onClick={() => onPageChange(totalPages - 1)}>
        Last
      </button>
    </div>
  )
}
