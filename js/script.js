function getCityInfo() {
    const cityName = document.getElementById('cityName').value;
    fetch(`/city_info?city_name=${cityName}`)
        .then(response => response.json())
        .then(data => {
            displayCityInfo(data);
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

function displayCityInfo(cityData) {
    const cityInfoDiv = document.getElementById('cityInfo');
    cityInfoDiv.innerHTML = ''; // 清空先前的内容
    if (cityData.message) {
        cityInfoDiv.innerText = cityData.message;
    } else {
        cityData.forEach(city => {
            const paragraph = document.createElement('p');
            paragraph.innerText = `City Name: ${city.cityname}, Latitude: ${city.lat}, Longitude: ${city.lng}`;
            cityInfoDiv.appendChild(paragraph);
        });
    }
}