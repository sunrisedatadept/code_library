// Credit to Evan Mceldowney for developing this Javascript
// Copy and paste this code into your EveryAction Online Actions themes template
// The code to rename race terms does not change the language in the backend

<script>
	var alterGenderandRace = function(args) {
		// Removing unneeded gender types
		var optionIds = [1,3,4,5,6,7,9,10,11,14,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,33,34,35,36,37,38,39,40,41,42,49,50,54,55];	
		optionIds.forEach( id => document.querySelector('select[name=Genders] option[value="' + id + '"]').remove() );

		// Rename a Race publicly (but will map to the existing value in the database)
		document.querySelector('select[name=Race] option[value="3"]').innerText = 'Caucasian/White';
		document.querySelector('select[name=Race] option[value="2"]').innerText = 'Black/African American';
		document.querySelector('select[name=Race] option[value="1"]').innerText = 'Asian/Asian American';
		document.querySelector('select[name=Race] option[value="4"]').innerText = 'Native American/First Nations/Alaska Native';
		document.querySelector('select[name=Race] option[value="10"]').innerText = 'Latino/Latina/Latinx';
	}
	var nvtag_callbacks = nvtag_callbacks || {};
	nvtag_callbacks.postRender = nvtag_callbacks.postRender || [];
	nvtag_callbacks.postRender.push(alterGenderandRace);
</script>

