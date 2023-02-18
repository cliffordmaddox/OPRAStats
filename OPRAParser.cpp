#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <time.h>
#include <map>

using namespace std;

class PCAPFileHeader // 24 Bytes
{
   public:
   unsigned int   magicNumber_;
   unsigned short majorVersion_;
   unsigned short minorVersion_;
   unsigned int   timeZone_;
   unsigned int   timestampAccuracy_;
   unsigned int   snapLength_;
   unsigned int   linkLayerType_;
   void print() { 
      cout << "magicNumber: " << hex << magicNumber_ << dec << endl;
      cout << "Version: " << majorVersion_ <<"." << minorVersion_ << endl;
      cout << "timeZone: " << timeZone_ << endl;
      cout << "timestampAccuracy: " << timestampAccuracy_ << endl;
      cout << "SnapLength: " << snapLength_ << endl;
      cout << "linkLayerType: " << linkLayerType_ << endl;
    }
};

class PCAPPacketHeader // 16 bytes
{
   public:
   unsigned int seconds_;
   unsigned int microSeconds_;
   unsigned int capturedLength_;
   unsigned int originalLength_;
   void print() { 
      cout << "timestamp: " << seconds_ << ":" << microSeconds_ 
         << "Length: " << hex << capturedLength_ << ", " << hex << originalLength_ << dec << endl;
    }
};


class OPRABlockHeader // 21 Bytes
{
   public:
   unsigned char  stuff_[42];  
   unsigned char  version_; //always 6
   unsigned char  size_[2];
   unsigned char  dataFeedIndicator_;
   unsigned char  retransIndicator_;
   unsigned char  sessionIndicator_;
   unsigned char  sequenceNumber_[4];
   unsigned char  messageCount_;
   unsigned char  timeStamp_[8];
   unsigned char  checkSum_[2];
   
   void print() 
   { 
      unsigned int seq;
      memcpy(&seq, sequenceNumber_, 4);
      seq = __builtin_bswap32(seq);
//      if ( seq % 100 == 0 )
      {
         unsigned int epoch; memcpy( &epoch, timeStamp_, 4 );  
         unsigned int nano;memcpy( &nano, &timeStamp_[4], 4 ); nano = __builtin_bswap32(nano);
         time_t tim = (unsigned long) __builtin_bswap32(epoch);
         tm* msgtime = localtime(&tim);
         if ( msgtime != nullptr )
         {
            char prev = cout.fill('0');
//            cout << "sequenceNumber: " << seq << " msgCount: " << (short) messageCount_ << " " << msgtime->tm_hour << ":" << setw(2) << msgtime->tm_min << ":" << setw(2) << msgtime->tm_sec << "." << (nano/1000000) <<  endl;
            cout << msgtime->tm_hour << ":" << right << setw(2) << msgtime->tm_min << ":" << right << setw(2) << msgtime->tm_sec << "." << setw(3) << (nano/1000000) << " ";// <<  endl;
            cout.fill(prev);
         }
      }
   }
};
class OPRAMessageHeader // 12 Bytes
{
   public:
   unsigned char  participantID_;
   unsigned char  category_;
   unsigned char  type_;
   unsigned char  indicator_;
   unsigned int   transactionID_;
   unsigned int   participantReferenceNumber_;
   void print() {
      cout << "Exchange: '" << participantID_ << "', category: '" << category_ << "', type: '" << type_ << "', indicator: '" << indicator_;
      cout << "', transactionId: " << hex<< transactionID_ << ", referenceNumber: " << participantReferenceNumber_ << dec << endl;
   }
};

class OPRALastSale // 31 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static short size_ = 31;
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   char  strike_[4];
   char  volume_[4];
   char  premiumPriceDenominator;
   char  PremiumPrice[4];
   char  TradeIdentifier[4];
   char  reserved2_[4];
};

class OPRAOpenInterest // 18 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static int size_ = 18;
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   char  strike_[4];
   char  volume_[4];
};

class OPRAEODSummary // 60 bytes
{
   public:
   OPRAMessageHeader header_;
   const static int size_ = 60;
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   char  strike_[4];
   char  volume_[4];
   char  openInterestVolume[4];
   char  premiumPriceDenominator;
   char  open[4];
   char  high[4];
   char  low[4];
   char  last[4];
   char  netChange[4];
   char  underlyingDenominator;
   char  underlyingPrice[8];
   char  bidPrice[4];
   char  offerPrice[4];
};

char buffer[100];

class OPRALongQuote // 31 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static int size_ = 31;
   char  symbol_[5];
   char  reserved_;
   char  expiration_[3];
   char  strikeDenominator_;
   char  strike_[4];
   char  premiumPriceDenominator_;
   char  bidPrice_[4];
   char  bidSize_[4];
   char  offerPrice_[4];
   char  offerSize_[4];

   string getInstrument() 
   {
      unsigned int temp;
      memcpy(&temp,strike_, 4 );
      temp = __builtin_bswap32( temp );
      memcpy(::buffer, symbol_, 5 );
      buffer[5] = expiration_[0];
      sprintf( &::buffer[6], "%02u%02u%08u%c", (unsigned short) expiration_[1], (unsigned short) expiration_[2], temp,header_.participantID_);
      return ::buffer;
   }
   void print()
   {
      unsigned int bidPrice;  memcpy( &bidPrice, bidPrice_, 4 );
      unsigned int bidSize; memcpy( &bidSize, bidSize_, 4 );
      unsigned int offerPrice; memcpy( &offerPrice, offerPrice_, 4 );
      unsigned int offerSize; memcpy( &offerSize, offerSize_, 4 );
      cout << getInstrument() <<  " " << strikeDenominator_ << "  "
         << setw( 4 ) << right << __builtin_bswap32( bidSize ) << right << setw( 9 ) << __builtin_bswap32( bidPrice ) << " " << left << setw( 9 )
         << __builtin_bswap32( offerPrice ) << setw(5) << __builtin_bswap32( offerSize ) << " " << premiumPriceDenominator_ << endl;
   }
};

class OPRAShortQuote // 17 Bytes
{
   public:
   OPRAMessageHeader header_;
   const static int size_ = 17;
   char  symbol_[4];
   char  expiration_[3];
   char  strike_[2];
   char  bidPrice_[2];
   char  bidSize_[2];
   char  offerPrice_[2];
   char  offerSize_[2];
   string getInstrument() 
   {
      unsigned short temp;
      memcpy(&temp,strike_, 2 );
      memcpy(::buffer, symbol_, 4 );
      temp = __builtin_bswap16( temp );
      ::buffer[4] = expiration_[0];
      sprintf( &::buffer[5], "%02u%02u%08u%c", (unsigned short) expiration_[1], (unsigned short) expiration_[2], temp, header_.participantID_);
      return ::buffer;
   }
   void print()
   {
      unsigned short bidPrice;  memcpy( &bidPrice, bidPrice_, 4 );
      unsigned short bidSize; memcpy( &bidSize, bidSize_, 4 );
      unsigned short offerPrice; memcpy( &offerPrice, offerPrice_, 4 );
      unsigned short offerSize; memcpy( &offerSize, offerSize_, 4 );
      cout << " " << getInstrument() << " A"  << "  "
         << setw( 4 ) << right << __builtin_bswap16( bidSize ) << right << setw( 9) << __builtin_bswap16( bidPrice ) << " " << left << setw( 9 )
         << __builtin_bswap16( offerPrice ) << setw( 5 ) << __builtin_bswap16( offerSize ) << " B" << endl;
   }
};

class OPRASingleAppendage //10 bytes
{
   public:
   char exchange_;
   char denominator_;
   char price_[4];
   char size_[4];
};

class OPRADoubleAppendage // 20 bytes
{
   public:
   char bidExchange_;
   char bidDenominator_;
   char bidPrice_[4];
   char bidSize_[4];
   char offerExchange_;
   char offerDenominator_;
   char offerPrice_[4];
   char offerSize_[4];
};

char scratchPad[200];

int main( int argc, char** argv )
{
   if ( argc < 2 )
   {
      cout << "Please specify a filename" << endl;
      return 1;
   }
   try
   {
      ifstream inputFile( argv[1], ifstream::in );
      PCAPFileHeader FileHeader;
      inputFile.read( (char*) &FileHeader, sizeof(FileHeader) );
      FileHeader.print();
      PCAPPacketHeader PacketHeader;
      unsigned long messageCount = 0;
      unsigned long tradeCount = 0;
      unsigned long quoteCount = 0;
      unsigned long bboCount = 0;
      unsigned long publishedMessageCount = 0;
      unsigned long publishedQuotes = 0;
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
      char buffer[100];

      map<string,unsigned int> longBidMap;
      map<string,unsigned int> longOfferMap;
      map<string,unsigned short> shortBidMap;
      map<string,unsigned short> shortOfferMap;
       
      while ( inputFile.read( (char*) &PacketHeader, sizeof(PacketHeader)))
      {
//         PacketHeader.print();
         int blockSize = 0;
         OPRABlockHeader BlockHeader;
         inputFile.read( (char*) &BlockHeader, sizeof (BlockHeader ) );
         blockSize += sizeof(BlockHeader);
//         BlockHeader.print();

         for ( int i = 0; i < BlockHeader.messageCount_; i++ )
         {
            OPRAMessageHeader* pHeader = (OPRAMessageHeader*) &scratchPad[0];
            inputFile.read((char*) pHeader, sizeof(OPRAMessageHeader));
            blockSize += sizeof(OPRAMessageHeader);
            messageCount++;
            perTypeCount[pHeader->category_] = perTypeCount[pHeader->category_] + 1;
//            Header.print();
            switch(pHeader->category_)
            {
               case 'H':
               {
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
                  publishedMessageCount++;
                  break;
               }
               case 'a':
               {
                  OPRALastSale* pLastSale = (OPRALastSale*) pHeader;
                  inputFile.read((char*) pLastSale->symbol_, pLastSale->size_);
                  blockSize += pLastSale->size_;
                  tradeCount++;
                  publishedMessageCount++;
                  break;
               }
               case 'd':
               {
                  OPRAOpenInterest* pOpenInterest = (OPRAOpenInterest*) pHeader;
                  inputFile.read((char*) pOpenInterest->symbol_, pOpenInterest->size_);
                  blockSize += pOpenInterest->size_;
                  publishedMessageCount++;
                  break;

               }
               case 'f':
               {
                  OPRAEODSummary* pEODSummary = (OPRAEODSummary*) pHeader;
                  inputFile.read((char*) pEODSummary->symbol_, pEODSummary->size_);
                  blockSize += pEODSummary->size_;
                  publishedMessageCount++;
                  break;
               }
               case 'k':   // long quote
               {
                  OPRALongQuote* pQuote = (OPRALongQuote*) pHeader;
                  inputFile.read((char*) pQuote->symbol_, pQuote->size_);
                  blockSize += pQuote->size_;
                  string sym = pQuote->getInstrument();
                  unsigned int bid;
                  memcpy(&bid, pQuote->bidPrice_, 4);
                  unsigned int offer;
                  memcpy(&offer, pQuote->offerPrice_, 4);
                  bid = __builtin_bswap32( bid );
                  offer = __builtin_bswap32( offer );
                  bool publish = false;
//                  if ( bid > 0 )
                  {
                     quoteCount++;
                     map<string, unsigned int>::iterator it = longBidMap.find(sym);
                     if (it != longBidMap.end())
                     {
                        if (it->second != bid)
                        {
                           publish = true;
                           it->second = bid;
                        }
                     }
                     else
                     {
                        publish = true;
                        longBidMap[sym] = bid;
                     }
                  }
//                  if ( offer > 0 )
                  {
//                     quoteCount++;
                     map<string, unsigned int>::iterator it = longOfferMap.find(sym);
                     if (it != longOfferMap.end())
                     {
                        if (it->second != offer)
                        {
                           publish = true;
                           it->second = offer;
                        }
                     }
                     else
                     {
                        publish = true;
                        longOfferMap[sym] = offer;
                     }
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
                     bboCount++;
                     publish = true;
                  }
                  else if ( pQuote->header_.indicator_=='O')
                  {
                     // both bid and offer are BBO.
                     OPRADoubleAppendage appendage;
                     inputFile.read((char*) &appendage, sizeof(appendage));
                     blockSize += sizeof(appendage);
                     publish = true;
                     bboCount+=2;
                  }
                  publishedMessageCount += publish;
                  publishedQuotes += publish;
//                  if ( publish )
                  {
                     BlockHeader.print();
                     pQuote->print();
                  }
                  break;
               }
               case 'q':   // short quote
               {
                  OPRAShortQuote* pQuote = (OPRAShortQuote*) pHeader;
                  inputFile.read((char*) pQuote->symbol_, pQuote->size_);
                  blockSize += pQuote->size_;
                  string sym = pQuote->getInstrument();
                  unsigned short bid;
                  memcpy(&bid, pQuote->bidPrice_, 2);
                  unsigned short offer;
                  memcpy(&offer, pQuote->offerPrice_, 2);
                  bid = __builtin_bswap16( bid );
                  offer = __builtin_bswap16( offer );
                  quoteCount++;
                  bool publish = false;
                  map<string, unsigned short>::iterator it = shortBidMap.find(sym);
                  if (it != shortBidMap.end())
                  {
                     if (it->second != bid)
                     {
                        publish = true;
                        it->second = bid;
                     }
                  }
                  else
                  {
                     publish = true;
                     shortBidMap[sym] = bid;
                  }
                  it = shortOfferMap.find(sym);
                  if (it != shortOfferMap.end())
                  {
                     if (it->second != offer)
                     {
                        publish = true;
                        it->second = offer;
                     }
                  }
                  else
                  {
                     publish = true;
                     shortOfferMap[sym] = offer;
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
                  publishedMessageCount += publish;
                  publishedQuotes += publish;
//                  if ( publish )
                  {
                     BlockHeader.print();
                     pQuote->print();
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
      cout << "longBid: " << longBidMap.size() << ", longOffer: " << longOfferMap.size() << ", shortbid: " << shortBidMap.size() << ", shortoffer: " << shortOfferMap.size() << endl;
      double reduction = (double) publishedQuotes/(double) quoteCount;
      cout << "messages:\t" << messageCount << endl;
      cout << "trades:\t\t" << tradeCount << endl;
      cout << "quotes:\t\t" << quoteCount << endl;
      cout << "BBO Count:\t" << bboCount << endl;
      cout << "pub Messages:\t" << publishedMessageCount << endl;
      cout << "pub Quotes:\t" << publishedQuotes << endl;
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
