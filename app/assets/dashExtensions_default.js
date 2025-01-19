window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, context) {
            const sel = context.hideout.selected || [];
            // Each feature has an 'id' property in feature.properties or feature.id
            // We'll assume 'feature.id' is the unique row index.
            let color = sel.includes(feature.id) ? 'red' : 'blue';
            return {
                color: color,
                fillColor: color,
                fillOpacity: 0.5,
                weight: 1
            }
        }

    }
});