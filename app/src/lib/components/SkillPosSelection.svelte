<script lang="ts">
	const skillPositions = ['QB', 'RB', 'WR', 'TE'];
	let { selected = $bindable() } = $props();

	function toggle(value: string) {
		selected = selected === value ? null : value;
	}
</script>

<div role="radiogroup" aria-label="Skill Position">
	{#each skillPositions as pos}
		<label>
			<button
				type="button"
				role="radio"
				aria-checked={selected === pos}
				class:selected={selected === pos}
				onclick={() => toggle(pos)}
			>
				{pos}
			</button>
		</label>
	{/each}
</div>

<style>
	div[role='radiogroup'] {
		display: flex;
		gap: 0.5rem;
	}

	button {
		padding: 0.5rem 1rem;
		border: 2px solid #ccc;
		border-radius: 6px;
		cursor: pointer;
		font-weight: 500;
		background: white;
		transition:
			background 0.2s,
			border-color 0.2s;
	}

	button.selected {
		background-color: #0070f3;
		color: white;
		border-color: #0070f3;
	}

	button:hover {
		border-color: #0070f3;
	}

	button:focus {
		outline: 2px solid #005bb5;
		outline-offset: 2px;
	}
</style>
