interface Props {
  score: number
  onChange: (score: number) => void
  disabled?: boolean
}

export default function StarRating({ score, onChange, disabled }: Props) {
  return (
    <div className="rating-wrapper">
      <div className="rating">
        {[5, 4, 3, 2, 1].map((star) => (
          <span
            key={star}
            className="rating-star"
            onClick={() => !disabled && onChange(star)}
            style={{ cursor: disabled ? 'default' : 'pointer' }}
          >
            {score >= star ? '★' : '☆'}
          </span>
        ))}
      </div>
      <div className="rating-score">{score}/5</div>
    </div>
  )
}
