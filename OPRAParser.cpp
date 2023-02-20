#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <time.h>
#include <map>

using namespace std;

char scratchPad[200];
char buffer[100];

string startTime;
string beginTime;

class PCAPFileHeader
{
   public:
   const static short _size_ = 24;
   istream& read(ifstream& file ) 
   {
      return file.read( (char*) &magicNumber_, _size_ );
   }
   void print() const { 
      cout << "magicNumber: " << hex << magicNumber_ << dec << endl;
      cout << "Version: " << majorVersion_ <<"." << minorVersion_ << endl;
      cout << "timeZone: " << timeZone_ << endl;
      cout << "timestampAccuracy: " << timestampAccuracy_ << endl;
      cout << "SnapLength: " << snapLength_ << endl;
      cout << "linkLayerType: " << linkLayerType_ << endl;
    }
   unsigned int   magicNumber_;
   unsigned short majorVersion_;
   unsigned short minorVersion_;
   unsigned int   timeZone_;
   unsigned int   timestampAccuracy_;
   unsigned int   snapLength_;
   unsigned int   linkLayerType_;
};

class PCAPPacketHeader // 16 bytes
{
   public:
   const static short _size_ = 16;
   unsigned int seconds_;
   unsigned int microSeconds_;
   unsigned int capturedLength_;
   unsigned int originalLength_;
   void print() const { 
      cout << "timestamp: " << seconds_ << ":" << microSeconds_ 
         << "Length: " << hex << capturedLength_ << ", " << hex << originalLength_ << dec << endl;
    }
};

class OPRABlockHeader // 21 Bytes
{
   public:
   const static short _size_ = 63;
   istream& read(ifstream& file );
   void printTime() const;
   unsigned char  version_; //always 6
   unsigned short blockSize_;
   unsigned char  dataFeedIndicator_;
   unsigned char  retransIndicator_;
   unsigned char  sessionIndicator_;
   unsigned int   sequenceNumber_;
   unsigned char  messageCount_;
   unsigned int   epoch_;
   unsigned int   nano_;
   private:
   unsigned short checkSum_; // not handled
};

istream& OPRABlockHeader::read(ifstream& file )
{
   file.read( buffer, 42 );
   file.read( (char*) &version_, 1 );  
   if (version_ != 6 )
   {
      cout << "Bad Version Number in Block Header" << endl;
      exit(1);
   }
   file.read( (char*) &blockSize_, 2); blockSize_ = __builtin_bswap16( blockSize_);
   file.read( (char*) &dataFeedIndicator_, 3 );
   file.read( (char*) &sequenceNumber_, 4 ); sequenceNumber_ = __builtin_bswap32( sequenceNumber_);
   file.read( (char*) &messageCount_, 1 );
   file.read( (char*) &epoch_, 8 ); epoch_ = __builtin_bswap32( epoch_ ); nano_ = __builtin_bswap32( nano_ );
   return file.read( (char*) &checkSum_, 2 );
}

void OPRABlockHeader::printTime() const
{ 
   time_t time = (unsigned long) epoch_;
   tm* msgtime = localtime( &time );
   if ( msgtime != nullptr )
   {
      char prev = cout.fill('0');
      cout << msgtime->tm_hour << ":" << right << setw(2) << msgtime->tm_min << ":" << right << setw(2) << msgtime->tm_sec << "." << setw(3) << (nano_/1000000) << " ";// <<  endl;
      cout.fill(prev);
      if (startTime.length() == 0 )
      {
         char temp[100];
         sprintf( temp, "%02d:%02d:%02d.%03d", msgtime->tm_hour, msgtime->tm_min, msgtime->tm_sec, nano_/1000000);
         startTime = temp;
      }
   }
}

class OPRAMessageHeader // 12 Bytes
{
   public:
   const static short _size_ = 12;
   unsigned char  participantID_;
   unsigned char  category_;
   unsigned char  type_;
   unsigned char  indicator_;
   unsigned int   transactionID_;
   unsigned int   participantReferenceNumber_;
   void print() const {
      cout << "Exchange: '" << participantID_ << "', category: '" << category_ << "', type: '" << type_ << "', indicator: '" << indicator_;
      cout << "', transactionId: " << hex<< transactionID_ << ", referenceNumber: " << participantReferenceNumber_ << dec << endl;
   }
};

class OPRALastSale // 31 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static short _size_ = 31;
   istream& read( ifstream& file )
   {
      file.read( symbol_, 10);
      file.read( (char*) &strike_, 9 ); strike_ = __builtin_bswap32(strike_); volume_ = __builtin_bswap32(volume_);
      file.read( (char*) &premiumPrice_, 12); premiumPrice_ = __builtin_bswap32( premiumPrice_ );
      return file;
   }
   void print() const 
   {
      cout << "Trade" << endl;
   }

   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   int   strike_;
   int   volume_;
   char  premiumPriceDenominator_;
   int   premiumPrice_;
   char  tradeIdentifier[4]; // 0x00000000
   char  reserved2_[4];
};

class OPRAOpenInterest // 18 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static int _size_ = 18;
   istream& read( ifstream& file )
   {
      file.read( symbol_, 10);
      file.read( (char*) &strike_, 8 ); strike_ = __builtin_bswap32(strike_); volume_ = __builtin_bswap32(volume_);
      return file;
   }

   void print() const 
   {
      cout << "OpenInterest" << endl;
   }
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   unsigned int strike_;
   unsigned int volume_;
};

class OPRAEODSummary // 60 bytes
{
   public:
   OPRAMessageHeader header_;
   const static int _size_ = 60;
   istream& read(ifstream& file )
   {
      return file.read( symbol_, _size_ );
   }
   void print() {}
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   unsigned int  strike_;
   unsigned int  volume_;
   unsigned int  openInterestVolume_;
   char  premiumPriceDenominator_;
   unsigned int open_;
   unsigned int high_;
   unsigned int low_;
   unsigned int last_;
   unsigned int netChange_;
   char  underlyingDenominator_;
   long  underlyingPrice_;
   unsigned int  bidPrice_;
   unsigned int  offerPrice_;
};

class OPRALongQuote // 31 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static int _size_ = 31;
   string getInstrument() const;
   void print() const;
   ifstream& read(ifstream& file );
   unsigned int getBidPrice() const { return bidPrice_; }
   unsigned int getBidSize() const { return bidSize_; }
   unsigned int getOfferPrice() const { return offerPrice_; }
   unsigned int getOfferSize() const { return offerSize_; }
   private:
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   unsigned int  strike_;
   char  premiumPriceDenominator_;
   unsigned int  bidPrice_;
   unsigned int  bidSize_;
   unsigned int  offerPrice_;
   unsigned int  offerSize_;
};

ifstream& OPRALongQuote::read(ifstream& file )
{
   file.read( (char*) symbol_, 10 );
   file.read( (char*) &strike_, 5); //slyly sneak in denominator in same read...
   strike_ = __builtin_bswap32( strike_ );
   file.read( (char*) &bidPrice_, 16 );
   bidPrice_ = __builtin_bswap32( bidPrice_ );
   bidSize_ = __builtin_bswap32( bidSize_ );
   offerPrice_ = __builtin_bswap32( offerPrice_ );
   offerSize_ = __builtin_bswap32( offerSize_ );
   return file;
}

string OPRALongQuote::getInstrument() const
{
   memcpy(::buffer, symbol_, 5 );
   buffer[5] = expiration_[0];
   sprintf( &::buffer[6], "%02u%02u%08u%c", (unsigned short) expiration_[1], (unsigned short) expiration_[2], strike_,header_.participantID_);
   return ::buffer;
}

void OPRALongQuote::print() const
{
      cout << getInstrument() <<  " " << strikeDenominator_ << "  "
         << setw( 4 ) << right << bidSize_ << right << setw( 9 ) << bidPrice_ << " " << left << setw( 9 )
         << offerPrice_ << setw(5) << offerSize_ << " " << premiumPriceDenominator_ << endl;
}

class OPRAShortQuote // 17 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static int _size_ = 17;
   string getInstrument() const;
   void print() const;
   ifstream& read(ifstream& file );
   unsigned short getBidPrice() const { return bidPrice_; }
   unsigned short getBidSize() const { return bidSize_; }
   unsigned short getOfferPrice() const { return offerPrice_; }
   unsigned short getOfferSize() const { return offerSize_; }
   private:
   char  symbol_[4];
   char  expiration_[3];
   unsigned short strike_;
   unsigned short bidPrice_;
   unsigned short bidSize_;
   unsigned short offerPrice_;
   unsigned short offerSize_;
   /*char  symbol_[4];
   char  expiration_[3];
   char  strike_[2];
   char  bidPrice_[2];
   char  bidSize_[2];
   char  offerPrice_[2];
   char  offerSize_[2];*/
};

ifstream& OPRAShortQuote::read(ifstream& file )
{
   file.read( (char*) symbol_, 7 );
   file.read( (char*) &strike_, 10);
   strike_ = __builtin_bswap16( strike_ );
   bidPrice_ = __builtin_bswap16( bidPrice_ );
   bidSize_ = __builtin_bswap16( bidSize_ );
   offerPrice_ = __builtin_bswap16( offerPrice_ );
   offerSize_ = __builtin_bswap16( offerSize_ );
   return file;
}


string OPRAShortQuote::getInstrument() const
{
   memcpy(::buffer, symbol_, 4 );
   ::buffer[4] = ' ';
   ::buffer[5] = expiration_[0];
   sprintf( &::buffer[6], "%02u%02u%08u%c", (unsigned short) expiration_[1], (unsigned short) expiration_[2], strike_, header_.participantID_);
   return ::buffer;
}

void OPRAShortQuote::print() const
{
   cout << getInstrument() << " A"  << "  "
      << setw( 4 ) << right << bidSize_ << right << setw( 9) << bidPrice_ << " " << left << setw( 9 )
      << offerPrice_ << setw( 5 ) << offerSize_ << " B" << endl;
}

class QuoteDetail 
{
   public:
   QuoteDetail( const QuoteDetail& src ) :
      bid_(src.bid_),
      offer_(src.offer_),
      bidSize_(src.bidSize_),
      offerSize_(src.offerSize_){}
   QuoteDetail(const OPRAShortQuote& src ) :
      bid_(src.getBidPrice()),
      offer_(src.getOfferPrice()),
      bidSize_(src.getOfferPrice()),
      offerSize_(src.getOfferSize()) {}
   QuoteDetail(const OPRALongQuote& src ) :
      bid_(src.getBidPrice()),
      offer_(src.getOfferPrice()),
      bidSize_(src.getOfferPrice()),
      offerSize_(src.getOfferSize()) {}
   QuoteDetail() : 
      bid_(0),
      offer_(0),
      bidSize_(0),
      offerSize_(0)
   {
   }
   unsigned int bid_;
   unsigned int offer_;
   unsigned short bidSize_; // it may be possible a size is bigger than 65535?
   unsigned short offerSize_;
};

class OPRASingleAppendage //10 bytes
{
   public:
   const static short _size_ = 10;
   char exchange_;
   char denominator_;
   char price_[4];
   char size_[4];
};

class OPRADoubleAppendage // 20 bytes
{
   public:
   const static short _size_ = OPRASingleAppendage::_size_ * 2;
   OPRASingleAppendage bid_;
   OPRASingleAppendage offer_;
};


int main( int argc, char** argv )
{
   if ( argc < 2 )
   {
      cout << "command line: " << endl << "\tOPRAParser filename [minsizechange]" << endl;
      return 1;
   }
   try
   {
      time_t now = time(0);
      beginTime = asctime( localtime(&now) );

      ifstream inputFile( argv[1], ifstream::in );
      unsigned int minSize = 0;
      if ( argc > 2 )
      {
         minSize = atoi(argv[2]);
         minSize > 65535? 65535:minSize;
      }
      PCAPFileHeader FileHeader;
      FileHeader.read( inputFile );
      FileHeader.print();

      PCAPPacketHeader PacketHeader;
      unsigned long messageCount = 0;
      unsigned long tradeCount = 0;
      unsigned long quoteCount = 0;
      unsigned long bboCount = 0;
      unsigned long publishedQuotes = 0;
      unsigned int SequenceNumber = 0;
      unsigned int Gaps = 0;

      map<char, unsigned int> perTypeCount;
      perTypeCount['H'] = 0;
      perTypeCount['a'] = 0;
      perTypeCount['d'] = 0;
      perTypeCount['f'] = 0;
      perTypeCount['k'] = 0;
      perTypeCount['q'] = 0;
      perTypeCount['H'] = 0;
      perTypeCount['Y'] = 0;
      perTypeCount['C'] = 0;

      map<string,QuoteDetail> SymMap;
      OPRABlockHeader BlockHeader;
       
      while ( inputFile.read( (char*) &PacketHeader, PacketHeader._size_ ) )
      {
         int blockSize = 0;
         BlockHeader.read( inputFile );
         blockSize += BlockHeader._size_;
         unsigned int curSeqNum = BlockHeader.sequenceNumber_;
         if ( curSeqNum < SequenceNumber )
         {
            cout << "Bad Sequence Number, last=" << SequenceNumber << ", current=" << curSeqNum << endl;
            return 1;
         }
         SequenceNumber++;
         if (curSeqNum != SequenceNumber)
         {
//            cout << "Gap Detected, last=" << SequenceNumber - 1  << ", current=" << curSeqNum << endl;
            Gaps++;
            SequenceNumber = curSeqNum;
         }
         for ( int i = 0; i < BlockHeader.messageCount_; i++ )
         {
            OPRAMessageHeader* pHeader = (OPRAMessageHeader*) &scratchPad[0];
            inputFile.read( (char*) pHeader, pHeader->_size_ );
            blockSize += pHeader->_size_;
            messageCount++;
            perTypeCount[pHeader->category_] = perTypeCount[pHeader->category_] + 1;
            switch(pHeader->category_)
            {
               case 'H':
               { 
                  BlockHeader.printTime();
                  if ( pHeader->type_ == 'C')
                     cout << "***START OF DAY" << endl;
                  else if ( pHeader->type_ == 'E')
                     cout << "***Start of Summary" << endl;
                  else if ( pHeader->type_ == 'F')
                     cout << "***End of Summary" << endl;
                  else if ( pHeader->type_ == 'J')
                     cout << "***END OF DAY" << endl;
                  else if ( pHeader->type_ == 'K')
                     cout << "***Reset Block Sequence Number" << endl;
                  else if ( pHeader->type_ == 'L')
                     cout << "***Start of Open Interest" << endl;
                  else if ( pHeader->type_ == 'M')
                     cout << "***End of Open Interest" << endl;
                  else if ( pHeader->type_ == 'N')
                     cout << "***Line Integrity" << endl;
                  else if ( pHeader->type_ == 'P')
                     cout << "***Disaster Recovery Data Center Activation" << endl;
                  else
                     cout << "***Unknown Type" << endl;
                  break;
               }
               case 'a':
               {
                  OPRALastSale* pLastSale = (OPRALastSale*) pHeader;
                  inputFile.read((char*) pLastSale->symbol_, pLastSale->_size_);
                  blockSize += pLastSale->_size_;
                  tradeCount++;
                  break;
               }
               case 'd':
               {
                  OPRAOpenInterest* pOpenInterest = (OPRAOpenInterest*) pHeader;
                  inputFile.read((char*) pOpenInterest->symbol_, pOpenInterest->_size_);
                  blockSize += pOpenInterest->_size_;
                  break;
               }
               case 'f':
               {
                  OPRAEODSummary* pEODSummary = (OPRAEODSummary*) pHeader;
                  inputFile.read((char*) pEODSummary->symbol_, pEODSummary->_size_);
                  blockSize += pEODSummary->_size_;
                  break;
               }
               case 'k':   // long quote
               {
                  OPRALongQuote* pQuote = (OPRALongQuote*) pHeader;
                  pQuote->read( inputFile );
                  blockSize += pQuote->_size_;
                  string sym = pQuote->getInstrument();
                  bool publish = false;
                  auto iter = SymMap.find( sym );
                  if ( iter != SymMap.end() )
                  {
                     if ( 0 < minSize )
                     {
                        if ( iter->second.bid_ != pQuote->getBidPrice() ||
                           abs((int) iter->second.bidSize_ - (int) pQuote->getBidSize()) > minSize ) 
                        {
                           publish = true;
                           iter->second.bid_ = pQuote->getBidPrice();
                           iter->second.bidSize_ = pQuote->getBidSize();
                        }
                        if (iter->second.offer_ != pQuote->getOfferPrice() ||
                           abs((int) iter->second.offerSize_ - (int) pQuote->getOfferSize()) > minSize ) 
                        {
                           publish = true;
                           iter->second.offer_ = pQuote->getOfferPrice();
                           iter->second.offerSize_ = pQuote->getOfferSize();
                        }
                     }
                     else
                     {
                        iter->second = *pQuote;
                        publish = true;
                     }
                  }
                  else // not currently stored.
                  {
                     publish = true;
                     SymMap[sym] = *pQuote;
                  }
                  if ( pQuote->header_.indicator_=='C' ||  
                     pQuote->header_.indicator_=='G' || 
                     pQuote->header_.indicator_=='K' ||
                     pQuote->header_.indicator_=='M' ||
                     pQuote->header_.indicator_=='N' || 
                     pQuote->header_.indicator_=='P' )
                  {
                     // single best bid offer;
                     OPRASingleAppendage appendage;
                     inputFile.read((char*) &appendage, appendage._size_);
                     blockSize += appendage._size_;
                     bboCount++;
                     publish = true;
                  }
                  else if ( pQuote->header_.indicator_=='O')
                  {
                     // both bid and offer are BBO.
                     OPRADoubleAppendage appendage;
                     inputFile.read((char*) &appendage, appendage._size_);
                     blockSize += appendage._size_;
                     publish = true;
                     bboCount+=2;
                  }
                  quoteCount++;
                  if ( publish )
                  {
                       publishedQuotes++;
//                     BlockHeader.printTime();
//                     pQuote->print();
                  }
                  break;
               }
               case 'q':   // short quote
               {
                  OPRAShortQuote* pQuote = (OPRAShortQuote*) pHeader;
                  pQuote->read( inputFile );
                  blockSize += pQuote->_size_;
                  string sym = pQuote->getInstrument();
                  bool publish = false;
                  auto iter = SymMap.find(sym);
                  if ( iter != SymMap.end() )
                  {
                     if ( 0 < minSize )
                     {
                        if ( iter->second.bid_ != pQuote->getBidPrice() ||
                           abs((int) iter->second.bidSize_ - (int) pQuote->getBidSize()) > minSize ) 
                        {
                           publish = true;
                           iter->second.bid_ = pQuote->getBidPrice();
                           iter->second.bidSize_ = pQuote->getBidSize();
                        }
                        if (iter->second.offer_ != pQuote->getOfferPrice() ||
                           abs((int) iter->second.offerSize_ - (int) pQuote->getOfferSize()) > minSize ) 
                        {
                           publish = true;
                           iter->second.offer_ = pQuote->getOfferPrice();
                           iter->second.offerSize_ = pQuote->getOfferSize();
                        }
                     }
                     else
                     {
                        iter->second = *pQuote;                        
                        publish = true;
                     }
                  }
                  else // not currently in dictionary.
                  {
                     publish = true;
                     SymMap[sym] = *pQuote;
                  }

                  if ( pQuote->header_.indicator_=='C' ||  
                     pQuote->header_.indicator_=='G' || 
                     pQuote->header_.indicator_=='K' ||
                     pQuote->header_.indicator_=='M' ||
                     pQuote->header_.indicator_=='N' || 
                     pQuote->header_.indicator_=='P' )
                  {
                     // single best bid offer;
                     OPRASingleAppendage appendage;
                     inputFile.read((char*) &appendage, sizeof(appendage));
                     blockSize += sizeof(appendage);
                     publish = true;
                     bboCount++;
                  }
                  else if ( pQuote->header_.indicator_=='O')
                  {
                     // both bid and offer are BBO.
                     OPRADoubleAppendage appendage;
                     inputFile.read((char*) &appendage, sizeof(appendage));
                     blockSize += sizeof(appendage);
                     publish = true;
                     bboCount += 2;
                  }
                  quoteCount++;
                  if ( publish )
                  {
                     publishedQuotes++;
//                     BlockHeader.print();
//                     pQuote->print();
                  }
                  break;
               }
               default:
                  cout << "Unhandled message: '" << pHeader->category_ << "', '" << pHeader->type_ << "'" << endl;
                  return 1;
            }
         }
         if (blockSize % 2 == 1 )
         {
            inputFile.read(buffer, 1); // padding on odd length blocks.
         }
         if ( PacketHeader.originalLength_ < PacketHeader.capturedLength_)
         {
//            cout << "extra bytes at end of packet " << PacketHeader.capturedLength_ - PacketHeader.originalLength_ << endl; 
            inputFile.read( buffer, PacketHeader.capturedLength_ - PacketHeader.originalLength_ );
         }
      }
      now = time(0);

      cout << "Start Time:\t" << beginTime;
      cout << "Finish Time:\t" << asctime( localtime(&now) ); 
      cout << "File startTime: " << startTime << ", endTime: ";
      BlockHeader.printTime();
      cout << endl;
      cout << "Symbol Count: " << SymMap.size() << endl;
      double reduction = (double) publishedQuotes/(double) quoteCount;
      cout << "messages:\t" << messageCount << endl;
      cout << "Gaps:\t\t" << Gaps << endl;
      cout << "trades:\t\t" << tradeCount << endl;
      cout << "quotes:\t\t" << quoteCount << endl;
      cout << "BBO Count:\t" << bboCount << endl;
      cout << "pub Quotes:\t" << publishedQuotes << endl;
      cout << "Conflating size only quotes minSize: " << minSize << endl;
      cout << "quote pub %:\t" << reduction << endl;

      for (auto it = perTypeCount.begin(); it != perTypeCount.end(); it++ )
      {
         cout << it->first << ": " << it->second << endl;
      }
   }
   catch(const std::exception& e)
   {
      cerr << e.what() << '\n';
      return 1;
   }
  
   return 0;
}
