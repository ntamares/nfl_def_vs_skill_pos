import { useState } from 'react';
import DefTeamSelection from './DefTeamSelection';
import { fetchSkillPosWeeklyTotal } from '../../api/def-vs-skill-pos/skill-pos-weekly-total';

type WeeklyTotal = {
  teamId: number;
  position: string;
  week: number;
  total: number;
};

function OptionsSelection(){
  const [selectedTeam, setSelectedTeam] = useState<number | null>(null);
  const [result, setResult] = useState<WeeklyTotal[] | null>(null);
  
  const handleSubmit = async () => {
    if (selectedTeam != null) {
      const data = await fetchSkillPosWeeklyTotal(selectedTeam, "QB", 1, 5);
      if (data != null) {
        setResult(data);
      }
    }
  }

  return (
    <>
      <DefTeamSelection
        value={selectedTeam}
        onChange={e => setSelectedTeam(e.target.value === '' ? null : Number(e.target.value))}
      />
      <button type="button" onClick={handleSubmit}>
        Submit
      </button>
    </>
  )
}

export default OptionsSelection;
