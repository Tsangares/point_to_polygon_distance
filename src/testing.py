
def get_distance_to_edges(traffic_stop,ct_shape):
    point = traffic_stop['geometry'].iloc[0]
    distances = [shape['geometry'].boundary.distance(point) for shape in ct_shape.to_dict('records')]
    head = ct_shape.sample(1)
    distance = head.boundary.distance(point).iloc[0]
    ax = head['geometry'].plot()
    traffic_stop['geometry'].plot(ax=ax,marker='o',color="red")
    centroid = head.centroid
    centroid.plot(ax=ax,color='cyan',marker='x')
    centroid = centroid.iloc[0]
    start = np.array(point.coords)[0]
    end = np.array(centroid.coords)[0]
    unit_vector = (end-start)/np.linalg.norm(end-start)
    actual_end = unit_vector*distance + start
    x,y = zip(*[start,end])
    ax.plot(x,y)
    x,y = zip(*[start,actual_end])
    print(start,end,actual_end,distance)
    ax.plot(x,y)
    plt.show()
    #return distance