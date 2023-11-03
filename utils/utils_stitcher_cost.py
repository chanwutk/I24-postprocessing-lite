'''
data_association module connected with database
3/25: first pass of spatial_temporal_match_online ready
- parallel processes?
- how to set up queues?
'''
import numpy as np
# import torch
# from scipy import stats
from i24_logger.log_writer import catch_critical
from utils.misc import calc_fit_select, calc_fit_select_ransac
import statsmodels.api as sm
import warnings
warnings.filterwarnings('error')

dt=0.04

def bhattacharyya_distance(mu1, mu2, cov1, cov2):
    mu = mu1-mu2
    cov = (cov1+cov2)/2
    # TODO: dets are zeros! (or very close to zeros)
    det = np.linalg.det(cov)
    det1 = np.linalg.det(cov1)
    det2 = np.linalg.det(cov2)
    # Danger! could also overflow -> max value numpy can handle is 1.7976931348623157e+308
    # det1 * det2 could exceed the max value!!!

    dist = 0.125 * np.dot(np.dot(mu.T, np.linalg.inv(cov)), mu) + 0.5 * np.log(det/(np.sqrt(det1) * np.sqrt(det2)))
    
    if dist < -999:
        return 999
    else:
        return dist
    

def bhattacharyya_coeff(bhatt_dist):
    return np.exp(-bhatt_dist)

def weighted_least_squares(t,x,y,weights=None):
    '''
    '''
    t = sm.add_constant(t)
    modelx = sm.WLS(x, t, weights=weights)
    resx = modelx.fit()
    fitx = [resx.params[1],resx.params[0]]
    modely = sm.WLS(y, t, weights=weights)
    resy = modely.fit()
    fity = [resy.params[1],resy.params[0]]
    return fitx, fity

@catch_critical(errors = (Exception))
def stitch_cost(track1, track2, TIME_WIN, param):
    '''
    use bhattacharyya_distance
    track t,x,y must not have nans!
    '''
    # print("compare ",track1["_id"], track2["_id"])
    t1 = track1["timestamp"] #[filter1]
    t2 = track2["timestamp"] #[filter2]
    
    # offset timestamps to avoid large numbers in weighted least squares fit
    toffset = min(t1[0], t2[0])
    t1 = t1-toffset
    t2 = t2-toffset
    
    gap = t2[0] - t1[-1] 
    if gap < 0 or gap > TIME_WIN:
        return 1e6
    
    x1 = track1["x_position"]#[filter1]
    x2 = track2["x_position"]#[filter2]
    
    y1 = track1["y_position"]#[filter1]
    y2 = track2["y_position"]#[filter2]

    n1 = min(len(t1), int(1/dt)) # for track1
    n2 = min(len(t2), int(1/dt)) # for track2
        
    if len(t1) >= len(t2):
        anchor = 1 # project forward in time
        direction = track1["direction"]
        # find the new fit for anchor1 based on the last ~1 sec of data
        t1 = t1[-n1:]
        x1 = x1[-n1:]
        y1 = y1[-n1:] # TODO: could run into the danger that the ends of a track has bad speed estimate
        # fitx, fity = calc_fit_select_ransac(t1,x1,y1,residual_threshold_x, residual_threshold_y)
        # fitx, fity = calc_fit_select(t1,x1,y1)
        weights = np.linspace(1e-6, 1, len(t1)) # put more weights towards end
        fitx, fity = weighted_least_squares(t1,x1,y1,weights)
        
        # get the first chunk of track2
        meast = t2[:n2]
        measx = x2[:n2]
        measy = y2[:n2]
        pt = t1[-1] # cone starts at the end of t1
        y0 = y1[-1]
        dir = 1 # cone should open to the +1 direction in time (predict track1 to future)
        
        
    else:
        anchor = 2
        direction = track2["direction"]
        # find the new fit for anchor2 based on the first ~1 sec of track2
        t2 = t2[:n2]
        x2 = x2[:n2]
        y2 = y2[:n2]
        
        # fitx, fity = calc_fit_select_ransac(t2,x2,y2,residual_threshold_x, residual_threshold_y)
        # fitx, fity = calc_fit_select(t2,x2,y2)
        weights = np.linspace(1, 1e-6, len(t2)) # put more weights towards front
        fitx, fity = weighted_least_squares(t2,x2,y2,weights)
        
        pt = t2[0]
        y0 = y2[0]
        # get the last chunk of tarck1
        meast = t1[-n1:]
        measx = x1[-n1:]
        measy = y1[-n1:]
        dir = -1 # use the fit of track2 to "predict" back in time
  
    # find where to start the cone
    try:
        tdiff = (meast - pt) * dir # tdiff should be positive
    except TypeError:
        meast = np.array(meast)
        tdiff = (meast - pt) * dir 
        
    # bound x-velocity to non-negative for each direction
    slope, intercept = fitx
    if slope * direction < 0:
        slope = 0
        if anchor==1:
            intercept = sum(weights * x1)/sum(weights)
        else:
            intercept = sum(weights * x2)/sum(weights)
    targetx = slope * meast + intercept
    slope, intercept = fity
    targety = slope * meast + intercept
    # targety = 0 * meast + y0
    # print(anchor, fitx, fity)
    cx, mx, cy, my = param["cx"], param["mx"], param["cy"], param["my"]

    # old
    # sigmax = (0.1 + tdiff * 0.01) * fitx[0] # 0.1,0.1, sigma in unit ft
    # sigmay = (1 + tdiff *0.1) * fity[0]
    
    # old rewrite
    # sigmax = (cx + mx * tdiff) * fitx[0]
    # sigmay = (cy + my * tdiff) * fity[0]
    # sigmay = cy + my*tdiff
    
    # new, can avoide bhatt_distance divided by zero
    sigmax = cx + mx * tdiff * abs(fitx[0])
    sigmay = cy + my * tdiff * abs(fity[0])  

    varx = sigmax**2
    vary_pred = sigmay**2
    vary_meas = np.var(measy)
    vary_meas = max(vary_meas, cy**2) # lower bound 

    # vectorize!
    n = len(meast)
    mu1 = np.hstack([targetx, targety]) # 1x 2n
    mu2 = np.hstack([measx, measy]) # 1 x 2n
    cov1 = np.diag(np.hstack([varx, vary_pred])) # 2n x 2n
    cov2 = np.diag(np.hstack([np.ones(n)*varx[0], np.ones(n)*vary_meas])) 
    

    try:
        bd = bhattacharyya_distance(mu1, mu2, cov1, cov2)
        nll = bd/n # mean
    except Exception as e:
        print("{} in stitch_cost for {} and {}, assigned cost=10e6".format(str(e), track1["_id"], track2["_id"]))
        try:
            print("{} merged_ids: {}".format(track1["_id"], track1["merged_ids"]))
            print("{} merged_ids: {}".format(track2["_id"], track2["merged_ids"]))
        except:
            pass
        return 1e6
    
    # time_cost = 0.01* (np.exp(gap) - 1)
    time_cost = 0.1 * gap
#     print("id1: {}, id2: {}, cost:{:.2f}".format(str(track1['_id'])[-4:], str(track2['_id'])[-4:], nll+time_cost))
    
    tot_cost = nll + time_cost 
        
    return tot_cost




@catch_critical(errors = (Exception))
def stitch_cost_simple_distance(track1, track2, TIME_WIN, param):
    """
    A simple distance metric ||p_i(t_e^i)-p_j(t_s^j)||_2^2
    """
    t1 = track1["timestamp"] #[filter1]
    t2 = track2["timestamp"] #[filter2]
    
    # offset timestamps to avoid large numbers in weighted least squares fit
    toffset = min(t1[0], t2[0])
    t1 = t1-toffset
    t2 = t2-toffset
    
    gap = t2[0] - t1[-1] 
    if gap < 0 or gap > TIME_WIN:
        return 1e6
    
    x1 = track1["x_position"]#[filter1]
    x2 = track2["x_position"]#[filter2]
    
    y1 = track1["y_position"]#[filter1]
    y2 = track2["y_position"]#[filter2]

    # n1 = min(len(t1), int(1/dt)) # for track1
    # n2 = min(len(t2), int(1/dt)) # for track2

    pi = np.array([x1[-1], y1[-1]])
    pj = np.array([x2[0], y2[0]])

    distance = np.linalg.norm(pi-pj)

    return distance



