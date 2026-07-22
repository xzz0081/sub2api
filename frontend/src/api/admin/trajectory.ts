import { apiClient } from '../client'

export interface TrajectoryModelBreakdown {
  model: string
  sessions: number
  calls: number
  input_tokens: number
  output_tokens: number
}

export interface TrajectoryKeyStatsResult {
  key: string
  found: boolean
  sessions: number
  calls: number
  input_tokens: number
  output_tokens: number
  models: TrajectoryModelBreakdown[]
  earliest_call?: string
  latest_call?: string
}

export async function getTrajectoryKeyStats(key: string): Promise<TrajectoryKeyStatsResult> {
  const { data } = await apiClient.get<TrajectoryKeyStatsResult>('/admin/trajectory/key-stats', {
    params: { key }
  })
  return data
}

export const trajectoryAPI = {
  getTrajectoryKeyStats
}

export default trajectoryAPI
