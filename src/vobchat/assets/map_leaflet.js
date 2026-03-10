window.map_leaflet = Object.assign({}, window.map_leaflet, {
    style_function: function (feature, context) {
        const sel = (context && context.hideout) ? (context.hideout.selected || []).map(String) : [];
        const withTheme = (context && context.hideout) ? (context.hideout.withTheme || []).map(String) : [];
        // Neutral greys used when no theme shading applies
        const GREY_FILL = '#9a9c9eff';
        const NEUTRAL_FILL_OPACITY = 0.0;
        const THEME_FILL_OPACITY = 0.2;
        const SELECTED_FILL_OPACITY = 0.7;

        const unitColors = {
            'CONSTITUENCY': 'green',
            'LG_DIST': 'orange',
            'MOD_CNTY': 'purple',
            'MOD_DIST': 'brown',
            'MOD_REG': 'blue',
            'MOD_WARD': 'darkgreen'
        };

        const unitType = feature?.properties?.g_unit_type || 'MOD_REG';
        const outlineColor = unitColors[unitType] || 'black';

        const rawId = (feature?.properties && (feature.properties.g_unit ?? feature.properties.id)) ?? feature.id;
        const featureIdStr = String(rawId);
        const isSelected = sel.includes(featureIdStr);
        // Compact conditional log: only when tracing is enabled and feature is selected
        try {
            if ((window.VOB_TRACE?.style || window.VOB_DEBUG) && isSelected) {
                const hasTheme = Array.isArray(withTheme) && withTheme.includes(featureIdStr);
                console.log('[style]', { id: featureIdStr, sel: isSelected ? 1 : 0, wt: hasTheme ? 1 : 0 });
            }
        } catch (_) {}

        if (isSelected) {
            // Strong highlight for selected polygons, but keep unit-type outline color
            return { color: outlineColor, fillColor: outlineColor, fillOpacity: SELECTED_FILL_OPACITY, weight: 3 };
        }
        // Default: neutral grey; if feature has data for selected theme, shade by unit color
        const hasThemeData = Array.isArray(withTheme) && withTheme.includes(featureIdStr);
        if (hasThemeData) {
            return {
                color: outlineColor, // always unit-type outline
                weight: 2,
                fillColor: outlineColor,
                fillOpacity: THEME_FILL_OPACITY,
            };
        }
        return {
            color: outlineColor, // always unit-type outline
            weight: 2,
            fillColor: GREY_FILL,
            fillOpacity: NEUTRAL_FILL_OPACITY
        };
    }
}); 
