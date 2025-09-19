export async function fetchData(defTeamId: number, skillPos: string) {
    let data: any = null;

    const url = `api/get-stats?defTeamId=${defTeamId}&skillPos=${skillPos}`;
    try {
        const response = await fetch(url);
        const result = await response.json();

        if (!response.ok) {
            console.error('API error response:', result);
            throw new Error('Failed to fetch data.');
        }

        return result;
    } catch (error) {
        console.error(error);

        // TODO set an error state here for UI
        data = null;
        return null;
    }
}