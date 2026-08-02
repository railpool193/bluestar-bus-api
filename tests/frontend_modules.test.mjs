import assert from 'node:assert/strict';
import {contrastText, departureState, routeColour, vehicleMeta} from '../static/js/formatters.js';
import {t, translationsComplete} from '../static/js/i18n.js';
import {normalizeRoute} from '../static/js/router.js';

assert.equal(routeColour('e40046'), '#e40046');
assert.equal(routeColour('not-a-colour'), '#1597dc');
assert.equal(contrastText('#000000'), '#ffffff');
assert.equal(contrastText('#ffffff'), '#071322');
assert.deepEqual(departureState({live: false}), {kind: 'timetable', labelKey: 'timetable'});
assert.equal(departureState({live: true, delayMinutes: 5}).labelKey, 'late');
assert.equal(departureState({live: true, delayMinutes: -3}).labelKey, 'early');
assert.equal(departureState({live: true, delayMinutes: 0}).labelKey, 'onTime');
assert.equal(t('nextStop', 'hu'), 'Következő megálló');
assert.equal(t('nextStop', 'en'), 'Next stop');
assert.equal(translationsComplete(), true);
assert.deepEqual(vehicleMeta({fleet: '1234', registration: 'AB12 CDE', vehicleType: 'E400', fuel: 'diesel'}), ['1234', 'AB12 CDE', 'E400', 'diesel']);
assert.deepEqual(vehicleMeta({fleet: '1234', registration: 'AB12 CDE', vehicleMetadataAmbiguous: true}), ['1234']);
assert.deepEqual(normalizeRoute('trip', {tripId: ''}), {view: 'home', params: {}});
assert.deepEqual(normalizeRoute('stop', {stopId: 'S1'}), {view: 'stop', params: {stopId: 'S1'}});

console.log('frontend module tests passed');
