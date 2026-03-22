import { useState, useEffect, useRef } from 'react'
import { MeiliSearch } from 'meilisearch'
import './index.css'

const MEILI_HOST = import.meta.env.VITE_MEILI_HOST || 'http://localhost:7700'
const MEILI_MASTER_KEY = import.meta.env.VITE_MEILI_MASTER_KEY || 'masterKey123!'
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:3000'

const client = new MeiliSearch({
  host: MEILI_HOST,
  apiKey: MEILI_MASTER_KEY,
})

function App() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState([])
  const [category, setCategory] = useState('')
  const [inStockOnly, setInStockOnly] = useState(false)
  const [feed, setFeed] = useState([])
  const [isFlash, setIsFlash] = useState(false)
  
  const timerRef = useRef(null)

  // Search logic
  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current)
    
    timerRef.current = setTimeout(() => {
      fetchResults()
    }, 300)

    return () => clearTimeout(timerRef.current)
  }, [query, category, inStockOnly])

  const fetchResults = async () => {
    try {
      let filter = []
      if (category) filter.push(`category = "${category}"`)
      if (inStockOnly) filter.push('in_stock = true')

      const res = await client.index('products').search(query, {
        filter: filter,
        limit: 50
      })
      setResults(res.hits)
    } catch (e) {
      console.error("Search error (maybe index not ready):", e)
    }
  }

  // SSE logic
  useEffect(() => {
    const sse = new EventSource(`${API_URL}/api/cdc-stream`)
    
    sse.addEventListener('cdc_event', (e) => {
      try {
        const data = JSON.parse(e.data)
        setFeed(prev => [data, ...prev].slice(0, 50)) // Keep last 50 events
        
        setIsFlash(true)
        setTimeout(() => setIsFlash(false), 500)
        
        // Auto-refresh search results if a relevant table changed
        if (data.table === 'products' || data.table === 'inventory') {
          setTimeout(fetchResults, 200) // small delay to let index update
        }
      } catch(err) {
        console.error("Error parsing event", err)
      }
    })

    return () => {
      sse.close()
    }
  }, [])

  return (
    <div className="app-container">
      {/* Live Feed Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <h2>Live CDC Stream</h2>
          <div className="live-indicator">
            <div className={`dot ${isFlash ? 'flash' : ''}`}></div>
            Listening
          </div>
        </div>
        <div className="feed-list">
          {feed.length === 0 && <p style={{color: '#64748b', textAlign: 'center', marginTop: '2rem'}}>Awaiting database events...</p>}
          {feed.map((event, idx) => (
            <div key={idx} className={`feed-item ${event.operation}`}>
              <span className="op-badge">{event.operation}</span>
              <div style={{fontSize: '0.8rem', marginBottom: '0.25rem', color: '#cbd5e1'}}>
                Table: <strong>{event.table}</strong>
              </div>
              <div className="feed-data">
                {JSON.stringify(event.data, null, 2)}
              </div>
            </div>
          ))}
        </div>
      </aside>

      {/* Main Search Area */}
      <main className="main-content">
        <header className="search-header">
          <h1>Product Catalog</h1>
          <p style={{color: '#94a3b8'}}>Real-time index synchronized directly from PostgreSQL WAL.</p>
        </header>

        <input 
          type="text" 
          className="search-bar" 
          placeholder="Search products..." 
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />

        <div className="filters">
          <div className="filter-group">
            <label style={{color: '#cbd5e1'}}>Category:</label>
            <select value={category} onChange={(e) => setCategory(e.target.value)}>
              <option value="">All Categories</option>
              <option value="Electronics">Electronics</option>
              <option value="Clothing">Clothing</option>
              <option value="Home">Home</option>
              <option value="Books">Books</option>
              <option value="Toys">Toys</option>
            </select>
          </div>
          <div className="filter-group">
            <label style={{color: '#cbd5e1', display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer'}}>
              <input 
                type="checkbox" 
                checked={inStockOnly} 
                onChange={(e) => setInStockOnly(e.target.checked)} 
              />
              In Stock Only
            </label>
          </div>
        </div>

        <div className="product-grid">
          {results.map(prod => (
            <div className="product-card" key={prod.id}>
              <div className="product-category">{prod.category}</div>
              <div className="product-name">{prod.name}</div>
              <div className="product-desc">{prod.description?.substring(0, 100)}...</div>
              <div className="product-footer">
                <div className="product-price">${Number(prod.price).toFixed(2)}</div>
                <div className={`product-stock ${prod.in_stock ? 'in-stock' : ''}`}>
                  {prod.in_stock ? `${prod.quantity} in stock` : 'Out of stock'}
                </div>
              </div>
            </div>
          ))}
          
          {results.length === 0 && (
            <div style={{gridColumn: '1 / -1', textAlign: 'center', padding: '3rem', color: '#64748b'}}>
              No products found. Start typing or wait for database seed.
            </div>
          )}
        </div>
      </main>
    </div>
  )
}

export default App
