<script>
     var alterGenderandRace = function(args) {
      // Removing unneeded gender types
     document.querySelector('select[name=Genders] option[value="18"]').remove();
     document.querySelector('select[name=Genders] option[value="19"]').remove();
     document.querySelector('select[name=Genders] option[value="1"]').remove();
     document.querySelector('select[name=Genders] option[value="20"]').remove();
     document.querySelector('select[name=Genders] option[value="21"]').remove();
     document.querySelector('select[name=Genders] option[value="54"]').remove();
     document.querySelector('select[name=Genders] option[value="4"]').remove();
     document.querySelector('select[name=Genders] option[value="22"]').remove();
     document.querySelector('select[name=Genders] option[value="5"]').remove();
     document.querySelector('select[name=Genders] option[value="6"]').remove();
     document.querySelector('select[name=Genders] option[value="23"]').remove();
     document.querySelector('select[name=Genders] option[value="24"]').remove();
     document.querySelector('select[name=Genders] option[value="25"]').remove();
     document.querySelector('select[name=Genders] option[value="7"]').remove();
     document.querySelector('select[name=Genders] option[value="26"]').remove();
     document.querySelector('select[name=Genders] option[value="27"]').remove();
     document.querySelector('select[name=Genders] option[value="9"]').remove();
     document.querySelector('select[name=Genders] option[value="10"]').remove();
     document.querySelector('select[name=Genders] option[value="28"]').remove();
     document.querySelector('select[name=Genders] option[value="3"]').remove();
     document.querySelector('select[name=Genders] option[value="11"]').remove();
     document.querySelector('select[name=Genders] option[value="29"]').remove();
     document.querySelector('select[name=Genders] option[value="30"]').remove();
     document.querySelector('select[name=Genders] option[value="31"]').remove();
     document.querySelector('select[name=Genders] option[value="33"]').remove();
     document.querySelector('select[name=Genders] option[value="34"]').remove();
     document.querySelector('select[name=Genders] option[value="35"]').remove();
     document.querySelector('select[name=Genders] option[value="14"]').remove();
     document.querySelector('select[name=Genders] option[value="36"]').remove();
     document.querySelector('select[name=Genders] option[value="16"]').remove();
     document.querySelector('select[name=Genders] option[value="37"]').remove();
     document.querySelector('select[name=Genders] option[value="38"]').remove();
     document.querySelector('select[name=Genders] option[value="39"]').remove();
     document.querySelector('select[name=Genders] option[value="40"]').remove();
     document.querySelector('select[name=Genders] option[value="41"]').remove();
     document.querySelector('select[name=Genders] option[value="42"]').remove();
     document.querySelector('select[name=Genders] option[value="49"]').remove();
     document.querySelector('select[name=Genders] option[value="17"]').remove();
     document.querySelector('select[name=Genders] option[value="55"]').remove();
     document.querySelector('select[name=Genders] option[value="50"]').remove();
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
    
