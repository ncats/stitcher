
function toArray(x, y, obj) {
   var data = [];
   for (var f in obj) {
      data.push({x: f, y: obj[f]});
   }
   //console.log(data);
   return data;
}

function loadMetricsPlots (_url) {
   $.ajax({
      url: _url,
      retryLimit: 5,
      tries: 0,
      success: function(d) {
          console.log('metrics: count='
		      +d.entityCount+' singletons='+d.singletonCount);
	  $('#entity-count').append(''+d.entityCount);
	  $('#singleton-count').append(''+d.singletonCount);
	  $('#component-count').append(''+d.connectedComponentCount);
	  $('#stitch-count').append(''+d.stitchCount);
	  
	  Morris.Bar({
	      element: 'entity-dist-plot',
	      data: toArray('x', 'y', d.entitySizeDistribution),
	      xkey: 'x',
	      ykeys: ['y'],
	      labels: ['Count']
	  });
	  
	  Morris.Bar({
	      element: 'stitch-dist-plot',
	      data: toArray('x', 'y', d.stitchHistogram),
	      xkey: 'x',
	      ykeys: ['y'],
	      axes: true,
	      hideHover: 'auto',
	      pointSize: 2,
	      barRatio: 0.4,
	      xLabelAngle: 35,
	      labels: ['Count']
	  });
	  
	  Morris.Bar({
	      element: 'component-dist-plot',
	      data: toArray('x', 'y', d.connectedComponentHistogram),
	      xkey: 'x',
	      ykeys: ['y'],
	      axes: true,
	      hideHover: 'auto',
	      pointSize: 2,
	      labels: ['Count']
	  });
	  
      },
       error: function(xhr, status, err) {
           console.log('error: '+err);
           if (xhr.status == 404) { // not ready
	       this.tries++;
	       if (this.tries <= this.retryLimit) {
		   var x = this;
		   setTimeout(function() {
                       console.log('retry..'+x.tries);
	               $.ajax(x);
		   }, 2000);
	       }
	   }
       }
   }).done(function(d) {
   });
}

function loadDataSourcePlot (_url, id) {
    $.ajax({
	url: _url,
	success: function (d) {
	    console.log(_url+'...'+d.length);
	    var _data = [];
	    //console.log('data sources: '+_data.length);

	    /*
	    for (var i = 0; i < d.length; ++i) {
		if (d[i].count) {
		    _data.push({ x: d[i].name, y: d[i].count});
		}
	    }

	    Morris.Bar({
		element: id,
		data: _data,
		xkey: 'x',
		ykeys: ['y'],
		axes: true,
		hideHover: 'auto',
		pointSize: 2,
		barRatio: 0.4,
		xLabelAngle: 35,
		labels: ['Count']
	    });
	    */
	    
	    for (var i = 0; i < d.length; ++i) {
		if (d[i].count) {
		    _data.push({ label: d[i].name, value: d[i].count});
		}
	    }
	    
	    Morris.Donut({
		element: id,
		data: _data
	    });
	},
	error: function (xhr, status, err) {
	    console.log('Error: status='+status+' error='+err);
	}
    }).done(function (d) {
    });
}
