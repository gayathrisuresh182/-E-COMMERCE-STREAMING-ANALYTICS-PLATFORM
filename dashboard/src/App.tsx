import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Home from './pages/Home'
import RealtimeOps from './pages/RealtimeOps'
import ABTesting from './pages/ABTesting'
import CustomerJourney from './pages/CustomerJourney'
import BusinessPerformance from './pages/BusinessPerformance'
import DataQuality from './pages/DataQuality'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Home />} />
        <Route path="realtime" element={<RealtimeOps />} />
        <Route path="experiments" element={<ABTesting />} />
        <Route path="funnel" element={<CustomerJourney />} />
        <Route path="business" element={<BusinessPerformance />} />
        <Route path="quality" element={<DataQuality />} />
      </Route>
    </Routes>
  )
}
