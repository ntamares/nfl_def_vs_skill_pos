export const config = {
  apiUrl: import.meta.env.VITE_API_URL,
  environment: import.meta.env.VITE_ENV,
  debug: import.meta.env.VITE_DEBUG === 'true',
  logLevel: import.meta.env.VITE_LOG_LEVEL,
};