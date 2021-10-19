'''
Created on 14 Feb 2021

@author: thomasgumbricht
'''

# Standard library imports

import os

import urllib.request

from sys import exit

from shutil import move

from html.parser import HTMLParser

# Third party imports

from ggtools.gg import print_gsm_date_coverage, gsm_download
 
# Package application imports

import geoimagine.support.karttur_dt as mj_dt

from geoimagine.ancillary import ProcessAncillary

from geoimagine.params import Struct, Composition, Location, LayerCommon, RegionLayer, VectorLayer, RasterLayer

class ProcessGrace():
    'class for Grace specific processes with the package ggtools'  
     
    def __init__(self, pp, session):
        '''
        '''
        
        self.session = session
                
        self.pp = pp  
        
        self.verbose = self.pp.process.verbose 
        
        self.session._SetVerbosity(self.verbose)

        if self.verbose > 0:

            print ('        ProcessGrace',self.pp.process.processid) 

        #Direct to SMAP sub-processes
        if self.pp.process.processid.lower() == 'searchgraceproducts':
            
            self._SearchGraceProducts()
            
        elif self.pp.process.processid.lower() == 'curlgrace':
            
            self._CurlGrace()
            
        if self.pp.process.processid.lower() == 'organizegrace':
            
            self._OrganizeGrace()
            
        else:
            
            exit(self.pp.process.processid)
        
    def _PrintGracaPeriod(self):
        '''
        '''
        
        for solutionset in ['CSR','GFZ','JPL']:
            print_gsm_date_coverage(solutionset,96)
            print_gsm_date_coverage(solutionset,60)
    
    def _DownloadGrace(self,publisher ):
        
        start_date = '2020-01'
        end_date = '2020-12'
        RL = 'RL06'
        pass
    
    #    gsm_download(source,D = 60,start_date = None,end_date = None,RL = 'RL06'): 
        '''
        Download GRACE GSM data from isdcftp.gfz-potsdam.de; if the file to be downloaded is already included in the download directory, the download is automatically skipped.
        
        Usage: 
        gsm_download('CSR', 96)
        gsm_download('GFZ', 96,'2003-01','2016-07')
        gsm_download('JPL', 60,'2006-05')
        gsm_download('JPL', end_date = '2018-12')
        '''

#To get the complete dataset

    def _GraceIndexPath(self):
        ''' Set paths to GRACE products
        '''
        
        # get the product, note the conflict between kartturs and smap namining convention
        self.feature = self.pp.process.parameters.feature
        
        self.solutionset = self.pp.process.parameters.solutionset
        
        self.model = self.pp.process.parameters.model
        
        self.version =self.pp.process.parameters.version
        
        #check that the version is correctly stated
        
        if not len(self.version) == 3 or not self.version[0] == 'v':
            
            exit('The GRACE version must be v0X digits, e.g. "v02" or "v03"')
               
        self.gracePath = os.path.join('L3','grace', self.feature, self.model, self.version, self.solutionset)
                
        if self.verbose > 1:
            
            print ('    Processing GRACE product.', self.gracePath)
            
        #create the localpath where the search data (html) will be saved
        self.localPath = os.path.join('/volumes',self.pp.dstPath.volume,'DAAC-GRACE',self.gracePath)
        
        if not os.path.exists(self.localPath):
            
            os.makedirs(self.localPath)
            
    def _SearchGraceProducts(self):
        '''IMPORTANT the user credentials must be in a hidden file in users home directory called ".netrc"
        '''
        
        # Set the path for this GRACE product
        self._GraceIndexPath()
                
        #Set the serverurl, and the Grace roduct and version to search for 
        self.serverurl = self.pp.process.parameters.serverurl
                                                   
        #change to the local directory
        
        cmd ='cd %s;' %(self.localPath)
        
        os.system(cmd)
                            
        # Define the complete url to the GRACE data
        url = os.path.join(self.serverurl,'drive','files','allData','tellus',self.gracePath,'')
                    
        indexFPN = os.path.join(self.localPath,'index.html')

        if os.path.exists(indexFPN) and not self.pp.process.overwrite:
            
            return
        
        cmd ='cd %s;' %(self.localPath)
        
        #Run the wget command including definition of the cookie needed for accessing the server
        # Updated in Feb 2021
        cmd ='%(cmd)s /usr/local/bin/wget -L --load-cookies ~/.grace_cookies --save-cookies ~/.grace_cookies --auth-no-challenge=on --keep-session-cookies --content-disposition %(url)s' %{'cmd':cmd, 'url':url}

        os.system(cmd)
        
    def _CurlGrace(self):
        ''' Curl Grace data via the downloaded index.html file in _SearchGraceProducts
        '''
        
        # Set the path for this GRACE product
        self._GraceIndexPath()
                       
        indexFPN = os.path.join(self.localPath,'index.html')
                      
        curlL = self._ReadGracehtml(indexFPN)
        
        home = os.path.expanduser("~")
        cookieFPN = os.path.join(home,'.grace_cookies')
        
        for curl in curlL:
            
            fpn = curl.replace('/drive/files/allData/tellus/L3/grace/','')
            
            curl = curl[1:len(curl)]
                        
            localFPN = os.path.join('/volumes',self.pp.dstPath.volume,'GRACE',fpn)
            
            if os.path.exists(localFPN) and not self.pp.process.overwrite:
                
                continue
            
            if not os.path.exists(os.path.split(localFPN)[0]):
                
                os.makedirs( os.path.split(localFPN)[0] )
                                        
            url = os.path.join( self.pp.process.parameters.serverurl, curl )
              
            cmd = "curl -n -L -c %(c)s -b %(c)s  %(r)s --output %(l)s;" %{'u':self.pp.process.parameters.remoteuser, 'c':cookieFPN, 'r':url, 'l':localFPN}
            
            os.system(cmd)
                       
    def _ReadGracehtml(self,srcFPN):
        '''
        '''
        
        queryD = self._ParseGraceWgetHTML(srcFPN)
        
        curlL = []
        
        ext = self.pp.srcPath.hdr.lower()
        
        if not ext[0] == '.':
            
            ext = '.%s' %(ext)
    
        for url in queryD:
                   
            if self.gracePath in url:
                                
                if self.pp.srcPath == '*':
                    
                    curlL.append(url)
                    
                elif os.path.splitext(url)[1].lower() == ext:
                
                    curlL.append(url)
                    
        return curlL
        
    def _ParseGraceWgetHTML(self, FPN):
        '''
        '''
        
        tmpFP = os.path.split(FPN)[0]
        
        tmpFP = os.path.split(tmpFP)[0]
        
        tmpFP = os.path.join(tmpFP,'tmpcsv')
        
        if not os.path.exists(tmpFP):
        
            os.makedirs(tmpFP)

        FPN = 'file://%(fpn)s' %{'fpn':FPN}
        
        req = urllib.request.Request(FPN)
        
        with urllib.request.urlopen(req) as response:
            
            html = response.read()
            
        parser = MjHTMLParser()

        parser.queryD = {}
        
        parser.feed(str(html)) 
        
        return (parser.queryD)
           
    def _SetGraceComp(self): 
        ''' Set the GRACE composition 
        '''      
        contentD = {'land_mass':'cm-water'}
        dataunitD = {'land_mass':'cm'}
        
        self.pp.process.parameters.importcode = self.pp.srcPath.hdr.lower()
        self.pp.process.parameters.epsg = 4326
        self.pp.process.parameters.orgid = 'NASA-GRACE'
        self.pp.process.parameters.dsname = 'GRACE-%s' %(self.pp.process.parameters.feature.replace('_','-'))
        self.pp.process.parameters.dsversion = '%s-%s' %(self.pp.process.parameters.model, self.pp.process.parameters.version)

        self.pp.process.parameters.regionid = 'global'
        self.pp.process.parameters.regioncat = 'global'
        
        self.pp.process.parameters.copyright = 'Open'
        
        dstCompD = {}
        dstCompD['source'] = "nasa-grace"
        
        dstCompD['content'] = self.pp.process.parameters.feature.replace('_','-')
        
        dstCompD['product'] = 'grace-%s' %( self.pp.process.parameters.solutionset.lower() )
        
        dstCompD['layerid'] = dstCompD['prefix'] = contentD[self.pp.process.parameters.feature]
        
        dstCompD['suffix'] = self.pp.process.parameters.dsversion
        dstCompD['scalefac'] = 1
        dstCompD['offsetadd'] = 0
        dstCompD['dataunit'] = dataunitD[self.pp.process.parameters.feature]
        dstCompD['celltype'] = "Float32"
        dstCompD['cellnull'] = self.pp.process.parameters.cellnull
        dstCompD['measure'] = "R"
        dstCompD['masked'] = "Y"
        
        self.comp = Composition(dstCompD, self.pp.process.parameters, self.pp.procsys.dstsystem, self.pp.procsys.dstdivision, self.pp.dstPath)    
        
        self.srcDataFP = os.path.join('GRACE',self.pp.process.parameters.feature,
                              self.pp.process.parameters.model,self.pp.process.parameters.version,
                            self.pp.process.parameters.solutionset)
        
        self.solutionSetFP = os.path.join('/Volumes',self.pp.srcPath.volume,self.srcDataFP)
        
        self.srcRawD = {}
        
        self.srcRawD['grace'] = {'datadir': self.srcDataFP, 'datalayer':'0',
                            'cellnull':self.pp.process.parameters.cellnull,
                            'title':self.pp.process.parameters.title,
                            'label':self.pp.process.parameters.label}
            
    def _OrganizeGrace(self):
        '''Identifies grace files in given folder and converts to single ancillary layer that is imported
        '''
        
        # Set the location - it is always global for grace
        locus = Location(self.pp.process.processid, self.pp.defregion, self.pp.procsys.dstsystem, self.pp.procsys.dstdivision, self.pp.procsys.dstepsg, self.session)
        
        # Set the composition - it is alawys the same for one processid
        self._SetGraceComp()
              
        # Craete a list for all layers  
        srcLayerL = []
        
        for fn in os.listdir(self.solutionSetFP):
            
            if fn.endswith(self.pp.srcPath.hdr):
                
                srcLayerL.append( fn )
        
        for fn in srcLayerL:
            
            # srcRaWD is constructed and then not sent, but instead recreated in ProcessAncilalry
            # This is becuase the filenmaes are from listdir and not constrcuted
            self.srcRawD['grace']['datafile'] = fn
                    
            self.pp.process.srcraw = [ Struct( self.srcRawD ) ]
            
            FNparts = fn.split('_')
                    
            yyyydoyStart, yyyydoyEnd = FNparts[1].split('-')
                    
            startDate = mj_dt.yyyydoyDate(yyyydoyStart)
                    
            endDate = mj_dt.yyyydoyDate(yyyydoyEnd)
           
            #Force the date to represent the first day of the month
            
            acqdate = mj_dt.ResetDateToYYYYMM01(startDate)
            
            acqdatestr = mj_dt.DateToYYYYMM(acqdate)
            
            self.datumL = [ {'acqdate':acqdate, 'acqdatestr':acqdatestr, 'timestep':'MS'} ]
        
            # Set the desitnation Compostion
            
            self.pp.dstCompD['grace'] = self.comp
            
            # Set the complete hiearchical layer definitiin in one go
            self.pp.dstLayerD = {'global': {acqdatestr: {} } }
                     
            self.pp.dstLayerD['global'][acqdatestr]['grace'] = RasterLayer(self.comp, locus.locusD['global'], self.datumL[0])

            ProcessAncillary(self.pp, self.session)
                
class MjHTMLParser(HTMLParser):
            
    def handle_starttag(self, tag, attrs):
        # Only parse the 'anchor' tag.
        if tag == "a":
            # Check the list of defined attributes.
            for name, value in attrs:
                                
                # If href is defined, print it.
                if name == "href" and '/drive/files/allData/tellus/L3/grace' in value:
                    
                    print (value)
                    
                    self.queryD[value]= value
                    
                                
#for solutionset in ['GFZ']:
#    dstFP = '/Volumes/karttur'
#    gsm_download(solutionset,dstFP,60,'2020-01','2020-12')
#    gsm_download(solutionset,dstFP,96,'2020-01','2020-12')
    

'''
#To get the complete dataset
for solutionset in ['CSR','GFZ','JPL']:
    gsm_download(solutionset,96) 
    gsm_download(solutionset,60)
'''
    
    

