export async function fetchSkillPosWeeklyTotal(teamId: number, position: string, startWeek: number, endWeek: number){
    const url: string = "http://localhost:5000/api/weekly-total";
    const params = {
        teamId, 
        position, 
        startWeek: startWeek ?? null, 
        endWeek: endWeek ?? null
    };
    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        body: JSON.stringify(params)
    });
    return await response.json();
}