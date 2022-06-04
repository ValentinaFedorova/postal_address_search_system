ymaps.ready(init);

function init() {

    var myMap = new ymaps.Map("map", {
            center: [55.76, 37.64],
            zoom: 10
        }, {
            searchControlProvider: 'yandex#search'
        });

    for (var i=0; i<geopoints.points.length;i++){
        myMap.geoObjects
        .add(new ymaps.Placemark([geopoints.points[i].lat, geopoints.points[i].lon], {
            balloonContentHeader: geopoints.points[i].address,
            balloonContentBody: '<a href="' + geopoints.points[i].url_address + '">'+ geopoints.points[i].url_address +'</a>'
        }, {
            preset: 'islands#icon',
            iconColor: '#0095b6'
        }))
    }
}
