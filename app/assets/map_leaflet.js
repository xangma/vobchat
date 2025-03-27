window.map_leaflet = Object.assign({}, window.map_leaflet, {
    style_function: function(feature, context) {
        // Add a fallback for context or hideout if they're undefined
        const sel = (context && context.hideout) ? context.hideout.selected || [] : [];

        // Mapping unit types to outline colors:
        const unitColors = {
            'CONSTITUENCY': 'green',
            'LG_DIST': 'orange',
            'MOD_CNTY': 'purple',
            'MOD_DIST': 'brown',
            'MOD_REG': 'blue'
        };

        let unitType = feature.properties.g_unit_type || 'MOD_REG';
        let outlineColor = unitColors[unitType] || 'black';

        if (sel.includes(feature.id)) {
            return {
                color: 'red',
                fillColor: 'red',
                fillOpacity: 0.5,
                weight: 2
            };
        } else {
            return {
                color: outlineColor,
                fillColor: 'transparent',
                fillOpacity: 0.0,
                weight: 2
            };
        }
    }

});