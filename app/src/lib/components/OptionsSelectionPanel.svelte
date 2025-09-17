<script lang="ts">
	import DefTeamSelection from './DefTeamSelection.svelte';
	import { sortedNflTeams } from '../../types/nfl-teams';
	let team: string = '';
	let defTeamId: number;
	let skillPos: string | null = null;

	$: filteredTeams = team
		? sortedNflTeams.filter((t) => t.name.toLowerCase().includes(team.toLowerCase()))
		: sortedNflTeams;

	const handleSubmit = async () => {
		if (defTeamId && skillPos) {
		}
	};
</script>

<DefTeamSelection bind:value={defTeamId} />
