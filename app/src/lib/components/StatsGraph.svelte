<script lang="ts">
	import { fetchData } from '../../routes/api/fetch-data';
	import * as d3 from 'd3';
	import { scaleLinear } from 'd3-scale';
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

	let width = $state(600);
	const height = 400;
	const padding = { top: 20, right: 20, bottom: 30, left: 40 };
	// TODO make yTicks dynamic based on (max?) fantasy points?
	// const yTicks = [-5, 0, 5, 10, 15, 20, 25, 30, 35, 40];

	/*
	let xScale = $derived(
		scaleLinear()
			.domain([0, weekNumbers.length])
			.range([padding.left, width - padding.right])
	);
	// let yScale = $derived(
	// 	scaleLinear()
	// 		.domain([0, Math.max.apply(null, yTicks)])
	// 		.range([height - padding.bottom, padding.top])
	// );
	// let yScale = $derived(
	// 	scaleLinear()
	// 		.domain([
	// 			Math.min(0, ...fantasyPoints), // handles negative values
	// 			Math.max(0, ...fantasyPoints)
	// 		])
	// 		.range([height - padding.bottom, padding.top])
	// );

	*/
	let innerWidth = $derived(width - (padding.left + padding.right));
	let barWidth = $derived(innerWidth / weekNumbers.length);

	let yTicks: Array<number> = [-5, 0, 5, 10, 15, 20, 25, 30, 35, 40];

	let yDomain = $derived(
		data && data.length > 0
			? [Math.min(0, ...fantasyPoints), Math.max(0, ...fantasyPoints)]
			: [Math.min(...yTicks), Math.max(...yTicks)]
	);

	let xScale = $derived(
		scaleLinear()
			.domain([0, weekNumbers.length])
			.range([padding.left, width - padding.right])
	);

	let yScale = $derived(
		scaleLinear()
			.domain(yDomain)
			.range([height - padding.bottom, padding.top])
	);

	// Shorten the date axis values for mobile
	function formatMobile(tick: string) {
		return "'" + tick.toString().slice(-2);
	}
</script>

<div class="chart" bind:clientWidth={width}>
	<svg {width} {height}>
		<!-- 4. Design the bars -->
		<g class="bars">
			{#each data as week, i}
				<rect
					x={xScale(i) + 2}
					y={yScale(week.fantasy_points)}
					width={barWidth * 0.9}
					height={Math.abs(yScale(0) - yScale(week.fantasy_points))}
				/>

				<!-- Circle showing the start of each Bar -->
				<circle cx={xScale(i) + 2} cy={yScale(week.fantasy_points)} fill="white" r="5" />
			{/each}
		</g>
		<!-- Design y axis -->
		<g class="axis y-axis">
			{#each yTicks as tick}
				<g class="tick tick-{tick}" transform="translate(0, {yScale(tick)})">
					<line x2="100%" />
					<text y="-4">{tick} {tick === 20 ? '' : ''}</text>
				</g>
			{/each}
		</g>

		<!-- Design x axis -->
		<g class="axis x-axis">
			{#each data as week, i}
				<g class="tick" transform="translate({xScale(i)}, {height})">
					<text x={barWidth / 2} y="-4">
						{width > 380 ? week.week_number : formatMobile(week.week_number)}</text
					>
				</g>
			{/each}
		</g>
	</svg>
</div>

<style>
	.x-axis .tick text {
		text-anchor: middle;
		color: black;
	}

	.bars rect {
		fill: blue;
		stroke: none;
	}

	.tick {
		font-family: Poppins, sans-serif;
		font-size: 0.725em;
		font-weight: 200;
		color: black;
	}

	.tick text {
		fill: black;
		text-anchor: start;
		color: black;
	}

	.tick line {
		stroke: black;
		stroke-dasharray: 2;
		opacity: 1;
	}

	.tick.tick-0 line {
		display: inline-block;
		stroke-dasharray: 0;
	}
</style>
