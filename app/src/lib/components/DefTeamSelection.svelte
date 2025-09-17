<script lang="ts">
	import { sortedNflTeams } from '../../types/nfl-teams';

	export let value: number;
</script>

<input list="sortedNflTeams" placeholder="Type or pick a team" />

<datalist>
	{#each sortedNflTeams as team}
		<option value={team.id}>{team.market} {team.name}</option>
	{/each}
</datalist>
