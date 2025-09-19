<script>
	import OptionsPanel from '$lib/components/OptionsPanel.svelte';

	let defTeamId = $state(null);
	let skillPos = $state(null);
</script>

<h1>Welcome to NFL Def. Vs. Skill Positions Data Visualizer</h1>
<OptionsPanel {defTeamId} {skillPos} />
