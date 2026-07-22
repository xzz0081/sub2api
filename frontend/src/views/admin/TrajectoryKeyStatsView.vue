<template>
  <div class="space-y-5">
    <!-- 搜索面板 -->
    <div class="rounded-3xl bg-white px-6 py-5 shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700">
      <div class="flex items-center gap-3 border-b border-gray-100 pb-4 dark:border-dark-700">
        <div class="flex h-8 w-8 items-center justify-center rounded-lg bg-indigo-50 dark:bg-indigo-900/30">
          <svg class="h-4 w-4 text-indigo-600 dark:text-indigo-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
        </div>
        <h2 class="text-sm font-semibold text-gray-900 dark:text-white">轨迹查询</h2>
      </div>
      <div class="mt-4 flex gap-2">
        <div class="relative flex-1">
          <div class="pointer-events-none absolute inset-y-0 left-3 flex items-center">
            <svg class="h-4 w-4 text-gray-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
            </svg>
          </div>
          <input
            v-model="keyInput"
            class="w-full rounded-xl border border-gray-200 bg-gray-50 py-2.5 pl-10 pr-4 text-sm text-gray-900 placeholder-gray-400 transition focus:border-indigo-400 focus:bg-white focus:outline-none focus:ring-2 focus:ring-indigo-500/20 dark:border-dark-600 dark:bg-dark-900 dark:text-white dark:placeholder-gray-500 dark:focus:border-indigo-500 dark:focus:bg-dark-800"
            placeholder="输入 API Key（如 sk-xxxx）"
            @keydown.enter="query"
          />
        </div>
        <button
          type="button"
          :disabled="loading || !keyInput.trim()"
          class="flex items-center gap-2 rounded-xl bg-indigo-600 px-5 py-2.5 text-sm font-medium text-white shadow-sm transition hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500/40 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-indigo-500 dark:hover:bg-indigo-600"
          @click="query"
        >
          <svg v-if="loading" class="h-4 w-4 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          <svg v-else class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          {{ loading ? '查询中' : '查询' }}
        </button>
      </div>
      <p v-if="errorMsg" class="mt-2 text-xs text-red-500 dark:text-red-400">{{ errorMsg }}</p>
    </div>

    <!-- key 不存在 -->
    <div
      v-if="result && !result.found"
      class="rounded-3xl bg-white px-6 py-10 text-center shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700"
    >
      <div class="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-2xl bg-gray-100 dark:bg-dark-700">
        <svg class="h-6 w-6 text-gray-400" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      </div>
      <p class="text-sm font-medium text-gray-700 dark:text-gray-300">未找到该 Key 的轨迹数据</p>
      <p class="mt-1 text-xs text-gray-400 dark:text-gray-500">目录不存在或采集器未开启</p>
    </div>

    <!-- 结果面板 -->
    <template v-if="result && result.found">
      <!-- 汇总 -->
      <div class="rounded-3xl bg-white shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700">
        <!-- Key + 时间 -->
        <div class="flex flex-wrap items-center justify-between gap-3 border-b border-gray-100 px-6 py-4 dark:border-dark-700">
          <div class="flex items-center gap-2 min-w-0">
            <div class="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-green-50 dark:bg-green-900/20">
              <svg class="h-3.5 w-3.5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
            <code class="truncate rounded-lg bg-gray-100 px-2.5 py-1 font-mono text-xs text-gray-700 dark:bg-dark-700 dark:text-gray-300">{{ result.key }}</code>
          </div>
          <div v-if="result.earliest_call || result.latest_call" class="flex items-center gap-1.5 text-xs text-gray-400 dark:text-gray-500">
            <svg class="h-3.5 w-3.5 shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <span v-if="result.earliest_call">{{ formatTimestamp(result.earliest_call) }}</span>
            <span v-if="result.earliest_call && result.latest_call">→</span>
            <span v-if="result.latest_call">{{ formatTimestamp(result.latest_call) }}</span>
          </div>
        </div>

        <!-- 2 核心指标 -->
        <div class="grid grid-cols-2 divide-x divide-gray-100 dark:divide-dark-700">
          <div class="flex flex-col items-center justify-center gap-1 px-6 py-8">
            <span class="text-xs font-medium uppercase tracking-wider text-gray-400 dark:text-gray-500">Sessions</span>
            <span class="text-4xl font-bold tabular-nums text-gray-900 dark:text-white">{{ result.sessions.toLocaleString() }}</span>
            <span class="text-xs text-gray-400 dark:text-gray-500">对话轮次</span>
          </div>
          <div class="flex flex-col items-center justify-center gap-1 px-6 py-8">
            <span class="text-xs font-medium uppercase tracking-wider text-gray-400 dark:text-gray-500">Calls</span>
            <span class="text-4xl font-bold tabular-nums text-indigo-600 dark:text-indigo-400">{{ result.calls.toLocaleString() }}</span>
            <span class="text-xs text-gray-400 dark:text-gray-500">LLM 请求次数</span>
          </div>
        </div>

        <!-- 均值 -->
        <div class="border-t border-gray-100 px-6 py-3 dark:border-dark-700">
          <p class="text-center text-xs text-gray-400 dark:text-gray-500">
            平均每 session <span class="font-semibold text-gray-700 dark:text-gray-300">{{ avgCallsPerSession }}</span> 次调用
          </p>
        </div>
      </div>

      <!-- 模型明细 -->
      <div class="rounded-3xl bg-white shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700 overflow-hidden">
        <div class="flex items-center gap-2 border-b border-gray-100 px-6 py-4 dark:border-dark-700">
          <svg class="h-4 w-4 text-gray-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 10h16M4 14h16M4 18h16" />
          </svg>
          <h3 class="text-sm font-semibold text-gray-900 dark:text-white">模型明细</h3>
          <span class="ml-auto rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-500 dark:bg-dark-700 dark:text-gray-400">{{ sortedModels.length }} 个模型</span>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full text-sm">
            <thead>
              <tr class="border-b border-gray-100 bg-gray-50/70 dark:border-dark-700 dark:bg-dark-900/50">
                <th class="py-3 pl-6 pr-4 text-left text-xs font-medium uppercase tracking-wider text-gray-400 dark:text-gray-500">模型</th>
                <th class="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-400 dark:text-gray-500">Sessions</th>
                <th class="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-400 dark:text-gray-500">Calls</th>
                <th class="py-3 pl-4 pr-6 text-right text-xs font-medium uppercase tracking-wider text-gray-400 dark:text-gray-500">占比</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-50 dark:divide-dark-700">
              <tr
                v-for="m in sortedModels"
                :key="m.model"
                class="transition-colors hover:bg-gray-50/60 dark:hover:bg-dark-700/40"
              >
                <td class="py-3.5 pl-6 pr-4">
                  <span class="inline-block max-w-[240px] truncate rounded-md bg-gray-100 px-2 py-0.5 font-mono text-xs text-gray-700 dark:bg-dark-700 dark:text-gray-300">{{ m.model }}</span>
                </td>
                <td class="px-4 py-3.5 text-right tabular-nums text-gray-600 dark:text-gray-400">{{ m.sessions.toLocaleString() }}</td>
                <td class="px-4 py-3.5 text-right tabular-nums font-medium text-indigo-600 dark:text-indigo-400">{{ m.calls.toLocaleString() }}</td>
                <td class="py-3.5 pl-4 pr-6">
                  <div class="flex items-center justify-end gap-2">
                    <div class="h-1.5 w-24 overflow-hidden rounded-full bg-gray-100 dark:bg-dark-700">
                      <div class="h-full rounded-full bg-indigo-400 transition-all" :style="{ width: callPercent(m.calls) + '%' }" />
                    </div>
                    <span class="w-10 text-right text-xs tabular-nums text-gray-400 dark:text-gray-500">{{ callPercent(m.calls).toFixed(1) }}%</span>
                  </div>
                </td>
              </tr>
            </tbody>
            <tfoot>
              <tr class="border-t-2 border-gray-200 bg-gray-50/80 dark:border-dark-600 dark:bg-dark-900/60">
                <td class="py-3.5 pl-6 pr-4 text-xs font-semibold text-gray-700 dark:text-gray-300">合计</td>
                <td class="px-4 py-3.5 text-right text-xs font-semibold tabular-nums text-gray-700 dark:text-gray-300">{{ result.sessions.toLocaleString() }}</td>
                <td class="px-4 py-3.5 text-right text-xs font-semibold tabular-nums text-indigo-600 dark:text-indigo-400">{{ result.calls.toLocaleString() }}</td>
                <td class="py-3.5 pl-4 pr-6 text-right text-xs font-semibold text-gray-700 dark:text-gray-300">100%</td>
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { getTrajectoryKeyStats } from '@/api/admin/trajectory'
import type { TrajectoryKeyStatsResult } from '@/api/admin/trajectory'

const keyInput = ref('')
const loading = ref(false)
const errorMsg = ref('')
const result = ref<TrajectoryKeyStatsResult | null>(null)

async function query() {
  const key = keyInput.value.trim()
  if (!key) return
  loading.value = true
  errorMsg.value = ''
  result.value = null
  try {
    result.value = await getTrajectoryKeyStats(key)
  } catch (e: any) {
    errorMsg.value = e?.response?.data?.error ?? e?.message ?? '查询失败'
  } finally {
    loading.value = false
  }
}

const sortedModels = computed(() => {
  if (!result.value?.models) return []
  return [...result.value.models].sort((a, b) => b.calls - a.calls)
})

const avgCallsPerSession = computed(() => {
  if (!result.value?.sessions) return '0'
  return (result.value.calls / result.value.sessions).toFixed(1)
})

function callPercent(calls: number): number {
  if (!result.value?.calls) return 0
  return (calls / result.value.calls) * 100
}

function formatTimestamp(ts: string): string {
  if (!ts) return ''
  return ts.replace(/T(\d{2})-(\d{2})-(\d{2})-\d+Z/, ' $1:$2:$3').replace('T', ' ') + ' UTC'
}
</script>
