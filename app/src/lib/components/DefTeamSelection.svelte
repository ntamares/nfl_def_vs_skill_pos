<script lang="ts">
	import { sortedNflTeams } from '../../types/nfl-teams';
	// let { value } = $props();
	export let value: number | null = null;
</script>

<select bind:value>
	<option></option>
	{#each sortedNflTeams as team}
		<option value={team.id}>{team.market} {team.name}</option>
	{/each}
</select>

<!-- <input list="sortedNflTeams" placeholder="Type or pick a team" />

<datalist>
	{#each sortedNflTeams as team}
		<option value={team.id}>{team.market} {team.name}</option>
	{/each}
</datalist> -->
