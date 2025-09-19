<script lang="ts">
	import { fetchData } from '../../routes/api/fetch-data';
	import * as d3 from 'd3';
	import * as Plot from '@observablehq/plot';

	const { defTeamId, skillPos } = $props<{
		defTeamId: number;
		skillPos: string;
	}>();

	let data = $state<any>(null);

	$effect(() => {
		if (defTeamId && skillPos) {
			(async () => {
				data = await fetchData(defTeamId, skillPos);
			})();
		} else {
			data = null;
		}
	});

	let weekNumbers: Array<number> = $derived(
		data ? (data as Array<{ week_number: number }>).map((d) => d.week_number) : []
	);

	let fantasyPoints: Array<number> = $derived(
		data ? (data as Array<{ fantasy_points: number }>).map((d) => d.fantasy_points) : []
	);

	const width = 600;
	const height = 400;
	const margin = { top: 20, right: 20, bottom: 30, left: 40 };
</script>

<h1>{defTeamId}</h1>
<h2>{skillPos}</h2>
{#if data && data.length > 0}
	<pre>{weekNumbers}</pre>
	<pre>{fantasyPoints}</pre>
{:else}
	<p>No data found.</p>
{/if}
