<script lang="ts">
	import DefTeamSelection from './DefTeamSelection.svelte';
	import { sortedNflTeams } from '../../types/nfl-teams';
	import SkillPosSelection from './SkillPosSelection.svelte';
	import StatsGraph from './StatsGraph.svelte';

	let { defTeamId, skillPos } = $props();

	// TODO fix combo selector
	// $: filteredTeams = team
	// 	? sortedNflTeams.filter((t) => t.name.toLowerCase().includes(team.toLowerCase()))
	// 	: sortedNflTeams;
</script>

<DefTeamSelection bind:value={defTeamId} />
<SkillPosSelection bind:selected={skillPos} />

<StatsGraph {defTeamId} {skillPos} />
