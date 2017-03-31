#!/usr/bin/env python

import numpy as np
import itertools

from grid import Grid
import tools


# A class that carries the logic of evaluating the energy, force and torque 
# of a pair of rigid molecules. The coordinates of each molecule are given
# in the form of Xcom and q, with Xcom being the Cartesian coordinates of the 
# center of mass, q being the quaternion representation of its orientation 
# wrt a reference pose. The class evaluates the EFTs for a pair of such 
# coordinates by 
#   1. Apply translational and rotational operations to the pair to align the 
#      COM of the first molecule with the origin and its orientation to the 
#      reference pose. 
#   2. Convert the modified Xcom and q of the second molecule into spherical 
#      coordinates.
#   3. Use the resulted six-dimensional coordinate to query a six-dimensional 
#      grid that stores precomputed EFTs.
#   4. Unapply rotation in step 1 to obtain correctly oriented forces and torques
class EFT_calculator:
    def __init__(self, order=2):
        self.mol = Water()
        self.grid = Grid()
        self.order = order # order of the interpolant, 1 for linear

    # Setup the grid structure. If provided with a data file, load it
    def setup(self, filename=None):
        if not filename:
            self.grid.setup()
        else:
            self.grid.load(filename) 

    # Given a calculator that evalulates the atomic coordinates of a pair,
    # use the results to fill the grid
    def fill_grid(self, calculator, filename='grid_data.txt'):
        def f(x):
            coor = self._spherical2Atomic(x)
            return calculator.eval(coor)
        if not self.grid.n:
            raise Exception('setup() before fill')
        self.grid.fill(f)
        self.grid.save(filename)

    def fill_with_QM(self, logfilelist):
        """ input filename is a file with all gird GAMESS result log in order."""
        loglist = open(logfilelist, 'r').readlines()
        for i in range(len(loglist)):
            loglist[i] = loglist[i].rstrip()
        i = 0
        for leaf, x in self.grid._gen_leaves_with_x():
            leaf.y, coord = self._parseQMlog(loglist[i]) #coord is not using here
            i += 1
            if i >=len(loglist):break

    def _parseQMlog(self, logname):
        """extract energy, force from GAMESS log file and 
        return (energy, force[0],force[1],force[2], torque[0],torque[1],torque[2])
        ni, nj is the atom num. of framgment i,j 
        """
        AU2KCAL = 23.0605*27.2116
        HperB2toque = 1185.82 # 1Hartree/Bohr = 1185.82 kcal/mol/Angstrom
        frgE1 = -76.2987810745 * AU2KCAL
        frgE2 = -76.2987810745 * AU2KCAL
        e = 0.0
        f = np.zeros(3)
        t = np.zeros(3)
        logf = open(logname, 'r')
        log = logf.readlines()
        logf.close()
        coords = []
        gradients = []
        for idx, i in enumerate(log):
            if i[0:13] == " INPUT CARD> " and len(i.split()) == 7:
                try:coords.append([float(i) for i in i.split()[4:7]])
                except ValueError:continue
            if 'E(MP2)=' in i : e = float(i.split()[1]) * AU2KCAL - frgE1 - frgE2
            if 'GRADIENT OF THE ENERGY' in i: 
                for gline in log[idx+4:idx+10]:
                    gradients.append([float(g) * HperB2toque for g in gline.split()[2:5]])
                break
        coords = np.array(coords)
        gradients = np.array(gradients)
        # from com => probe
        com1 = self.mol.getCOM(coords[3:])
        coord1 = coords[:3]
        grad1 = gradients[:3]
        for idx in range(len(grad1)):
            f += grad1[idx]
            t += np.cross(coord1[idx] - com1, grad1[idx])
        return np.array([e, f[0], f[1], f[2], t[0], t[1], t[2]]), coords
            
        
    # Evaluate the Xcom and q for a pair of mols by querying the grid
    def eval(self, Xcom0, q0, Xcom1, q1):
        # move COM of mol0 to origin
        X = Xcom1 - Xcom0
        # reorient to align mol0 with refCoor
        R = tools.q2R(q0)
        X = np.dot(X, R)
        q = tools.qdiv(q1, q0)
        # Use mirror symmetry of mol0 to move mol1 such that its COM has positive y and z values
        reflections = []
        qsub = q[1:]
        for i in self.mol.refl_axes:
            if X[i] < 0:
                X[i] = -X[i]
                # the following operation on q is equivalent to changing R to MRM 
                # i.e., the probe mol is reflected twice, once in the reference frame,
                # once in the molecular frame.
                qsub[i] = -qsub[i]
                qsub[:] = -qsub
                reflections.append(i)
        # Use mirror symmetry of mol1 to orient it such that it has positive q[0] and q[1] values
        if q[0] < 0:
            q = -q
        if q[1] < 0:
            q[0], q[1], q[2], q[3] = -q[1], q[0], q[3], -q[2]
        # convert X, q to polar coordinates
        r, phi, theta = tools.xyz2spherical(X)
        ophi1, ophi2, otheta = tools.q2spherical(q)
        coor = [r, phi, theta, ophi1, ophi2, otheta]
        # use the grid to obtain results
        eft = self.grid.interpolate(coor, self.order)
        ener = eft[0]
        force = eft[1:4]
        torque = eft[4:7]
        # Reverse the operations for mol0 mirror symmetry back
        for i in reflections:
            force[i] = -force[i]
            torque[i] = -torque[i]
            torque[:] = -torque
        # Reverse the reorientation applied to align mol0 with refCoor
        force[:] = np.dot(force, R.T)
        torque[:] = np.dot(torque, R.T)
        return eft

    # Generate atomic coordinates for mol pair for grid points along with
    # an id. The optional arguments can be used to specify a range for the id.
    # The coordinates are in the form of [XO0, XH0, XH0, XO1, XH1, XH1], where 0 indicates
    # the center molecule, 1 the probe molecule.
    def gen_atomic_coors(self, start=None, stop=None):
        if stop is None:
            if start is not None:
                raise Exception('Specify start and stop at the same time!')
            start = 0
            stop = self.grid.n
        gen_x = itertools.islice(self.grid.gen_x(), start, stop)
        for i in range(start, stop):
            x = gen_x.next()
            coors = self._spherical2Atomic(x)
            yield i, coors

    # Construct atomic coordinates for a pair from grid coordinate
    def _spherical2Atomic(self, coor):
        r, phi, theta, ophi1, ophi2, otheta = coor
        Xcom = tools.spherical2xyz(r, phi, theta)
        q = tools.spherical2q(ophi1, ophi2, otheta)
        coor = self.mol.Xq2Atomic(Xcom, q)
        return np.concatenate((self.mol.refCoor, coor), axis=0)


# A class that holds information related to the atomic structure of a water
# molecule. It also includes several methods that carries out operations 
# related to the atomic coordinates.
class Water:
    def __init__(self):
        self.mass = np.array([15.99900, 1.00800, 1.00800])
        refCoor = np.array([ [-0.06556939,   0.00000000,    0.00000000],
                             [0.52035943,    0.76114632,    0.00000000],
                             [0.52035943,   -0.76114632,    0.00000000] ])
        # The following code ensures that refCoor has COM at origin and orientation
        # aligned with the getR() method
        refCoor = refCoor - self.getCOM(refCoor)
        R = self.getR(refCoor) 
        refCoor = np.dot(refCoor, R)
        self.refCoor = refCoor
        # refl_axes is a list of indices of axes. Reflection along each of these
        # axes corresponds to a mirror symmetry of the molecule
        self.refl_axes = [1, 2]

    # Calculate the rotation matrix R that relates coors to self.refCoor
    # vec_in_reference_frame = R \dot vec_in_body_frame 
    # R(coors) \dot refCoor = coors - COM(coors)
    # This function defines the orientation of self.refCoor
    # Need to be consistent with self.refl_axes
    def getR(self, coors):
        coors = np.copy(coors)
        offset = coors[0]
        coors -= offset
        xvec = coors[1] + coors[2]
        zvec = np.cross(coors[1], coors[2])
        yvec = np.cross(zvec, xvec)
        xvec /= np.linalg.norm(xvec)
        yvec /= np.linalg.norm(yvec)
        zvec /= np.linalg.norm(zvec)
        R = np.array([xvec, yvec, zvec]).T 
        return R

    # Calculate the center of mass
    def getCOM(self, coors):
        return np.dot(self.mass, coors) / self.mass.sum()

    # Convert atomic coordinates to Xcom and q
    def atomic2Xq(self, coors):
        Xcom = self.getCOM(coors)
        R = self.getR(coors)
        q = tools.R2q(R)
        return Xcom, q

    # Given Xcom and q, rebuild the atomic coordinates
    def Xq2Atomic(self, Xcom, q):
        R = tools.q2R(q)
        coor = np.dot(self.refCoor, R.T)
        coor += Xcom
        return coor




