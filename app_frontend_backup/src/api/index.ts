import { config } from '../config';

export const fetchData = async (endpoint: string) => {
  try {
    const response = await fetch(`${config.apiUrl}/${endpoint}`);
    
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    if (config.debug) {
      console.error('API request failed:', error);
    }
    throw error;
  }
};