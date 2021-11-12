// Project #1: External Sorting 

#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <ctime>
using namespace std;

void Generate_Random_Text_File (string gen_file_name, int gen_file_size)
{
	// open file
	ofstream myfile;
	myfile.open(gen_file_name.c_str());
	int total_size_so_far = 0;
	int wordsize = 0;
	char letter;

	while(total_size_so_far < gen_file_size)
	{
		wordsize = 1 + rand()%14; // random word length
		while(total_size_so_far + wordsize > gen_file_size)
			wordsize = 1 + rand()%14; 
		for(int j = 0; j < wordsize; j++)
		{
			letter = 'a' + rand()%26;
			myfile << letter;
		}
		total_size_so_far += wordsize;
		if(total_size_so_far + 1 < gen_file_size)
		{	
			myfile << ' '; 
			total_size_so_far++;
		}
	}
	myfile.close();
	cout << "File " << gen_file_name << " has been generated!" << endl; 
}

int main()
{
	// User is asked if s/he would like to generate file
	char choice;
	cout << "-------------------------------------------------------\n";
	cout << "Welcome to Owais Naeem's AMAZING External Sort Program!\n";
	cout << "-------------------------------------------------------\n\n\n";
	cout << "Would you like to generate a random text file? (y/n) ";
	cin >> choice;
	if(choice == 'y')
	{
		// Generate random text file
		string gen_file_name; int gen_file_size;
		cout << "\n(1) Enter preferred name of the file (including extension): ";
		cin >> gen_file_name;
		cout << "(2) Enter preferred size of the file (in bytes): ";
		cin >> gen_file_size;
		Generate_Random_Text_File(gen_file_name, gen_file_size);
	}

	/* 
	STEP 1:	Pass-0 should read data from disk, namely N pages into B blocks, where each block is a page. 
			As N is much greater than B, multiple runs will be created where each run is a set of B pages
			that has been sorted. Total number of runs generated = roof(N/B). 
	Constraints:	(1) User can choose page size, default 4K (4016 bytes).
					(2) User can choose a number of memory blocks B.
	*/

	// Taking user input
	int page_size, memory_blocks;
	string input_file, output_file;
	cout << "\n\nTo begin the external sort procedure, please provide the following information: ";
	cout << "(1) Enter the input file name (including extension): ";
	cin >> input_file;
	cout << "(2) Enter the output file name (including extension): ";
	cin >> output_file;
	char page_size_choice;
	cout << "(3) The default page size is 4 KB, would you like to change it? (y/n) ";
	cin >> page_size_choice;
	if(page_size_choice == 'y')
	{
		cout << "	- Enter the page size (in bytes): ";
		cin >> page_size;
	}
	else if(page_size_choice == 'n')
	{
		page_size = 1024*4;
	}
	else
	{
		cout << "	- Please restart the program and enter a valid choice for the page size!" << endl;
	}
	cout << "(4) Enter the number of memory blocks: ";
	cin >> memory_blocks;

	// Begin Time:	
	const clock_t begin_time = clock();

	int TOTAL_MEM = page_size * memory_blocks; // B blocks worth of memory
	ifstream inFile;
	inFile.open(input_file.c_str());
	
	// Get size of the file
	inFile.seekg (0, inFile.end);
    int input_file_size = inFile.tellg();
    inFile.seekg (0, inFile.beg);
	cout << "The size of the file chosen is (in bytes): " << input_file_size << endl;
	
	// Compute required number of runs
	int runs_count;
	if(input_file_size % TOTAL_MEM > 0)
		runs_count = input_file_size/TOTAL_MEM + 1;
	else
		runs_count = input_file_size/TOTAL_MEM;
	cout << "Given the inputted constraints, the required number of runs for PASS 0 is: " << runs_count << endl;
	
	vector<string> runs_names;
	string run_name;
	for(int i = 0; i < runs_count; i++)	// create names for the roof(N/B) runs that will be created later
	{
		std::stringstream ss;
		ss << "run_" << i << ".txt";
		run_name = ss.str();
		runs_names.push_back(run_name);
	}
	int size_of_text = 0;
	int run_index = 0;
	ofstream outFile;
	string word;
	vector<string> data;				

	// Begin reading the input file
	cout << "\n\n\n----------------------------------------\n";
	cout << "PASS 0 is now starting...\n";
	cout << "----------------------------------------\n\n\n";
	cout << "File name "<< input_file << " is now being read... " << endl;
	
	
	while(!inFile.eof())
	{
		// end of file has NOT been reached
		inFile >> word;
		if((size_of_text + word.size()) < TOTAL_MEM)
		{
			data.push_back(word);
			size_of_text++;
			size_of_text += word.size();
		}
		else
		{
			sort(data.begin(), data.end());
			outFile.open(runs_names[run_index].c_str());	// create run file 
//			cout << runs_names[run_index] << endl;
			int data_size = data.size();
			for(int i = 0; i < (data_size- 1); i++)
			{
				outFile << data[i];
				outFile << " ";
			}
			outFile << data[data_size - 1];
			outFile.close();								// close run file
			data.clear();			
			run_index++;
			data.push_back(word);
			size_of_text = word.size();
		//	peeked = inFile.peek();
		//	if(size_of_text + word.size() < TOTAL_MEM)
		//		size_of_text++;
		}

	}
	if(data.size() > 0)
	{
		sort(data.begin(), data.end());					
		outFile.open(runs_names[run_index].c_str());	// create run file
//		cout << runs_names[run_index] << endl;
		int data_size = data.size();
		for(int i = 0; i < (data_size-1); i++)	
		{
			outFile << data[i]; 
			outFile << " ";
		}
		outFile << data[data_size - 1];
		outFile.close();								// close run file
		data.clear();									
		run_index++;
		size_of_text = 0;
	}
	inFile.close();

	cout << "\n\n----------------------------------------\n";
	cout << "PASS 0 has now ended!\n";
	cout << "----------------------------------------\n\n";

/*
	- Read run_1 through run_(B-1) worth of memory into all blocks except the last. 
	- Use the last memory block to merge the runs and write out to a new run to be utilized by the next Pass. 
	- The limit of using just one memory block necessitates periodic flushing into a new run once block is filled.
	- The next Pass again utilizes all but the last memory block to read pages from the B-1 runs created in the 
	  previous Pass in the same manner. 
	- This process continues until the kth Pass until just one run is built which is the sorted result.
*/

	// initialize number of future passes needed for looping later
	vector<int> runs_in_pass; // runs_in_pass[i] runs in Pass (i+1)

	// compute future runs needed and store them with names that will be used in the future:
	// at this point, runs_count runs have been created in Pass 0
	int runs_countdown = runs_count;
	int future_runs_num = 0;
	while(runs_countdown > (memory_blocks-1))
	{
		if(runs_countdown % (memory_blocks-1) > 0)
			runs_countdown = (runs_countdown/(memory_blocks - 1)) + 1;
		else
			runs_countdown = runs_countdown/(memory_blocks-1);
		runs_in_pass.push_back(runs_countdown);
		future_runs_num += runs_countdown;
	}
	if(runs_countdown>1)
	{
		future_runs_num++; 
		runs_in_pass.push_back(1);
	}

	// TESTING TO SEE IF THE NUMBER OF PASSES AND NUMBER OF RUNS IN EACH PASS IS CORRECT:
	/*
	for(int i = 0; i < runs_in_pass.size(); i++)
		cout << "For Pass " << ( i+1) << " the number of runs generated are: " << runs_in_pass[i] << endl;
	cout << "The total number of future runs needed is: " << future_runs_num << " runs." << endl;
	*/
	if(future_runs_num == 0)
	{
		string oldname = runs_names[0];
		string newname = output_file;
		int result = rename( oldname.c_str() , newname.c_str() );
	}
	else 
	{
		for(int i = runs_count; i < runs_count + future_runs_num; i++)
		{
			std::stringstream ss;
			ss << "run_" << i << ".txt";
			run_name = ss.str();
			runs_names.push_back(run_name);
//			cout << run_name << endl;
		}
		// Rename the final run file with the user inputted file-name:
		runs_names.pop_back();
		std::stringstream ss;
		ss << output_file;
		run_name = ss.str();
		runs_names.push_back(run_name);
	}
	// Each pass, except the last, generates many runs
	// each run to be generated merges (memory_blocks - 1) existing runs
	// each existing run has to have it's data loaded in the array, and pointer_indexes must be set to allow for merging
	int current_reading_run_index = 0;
	int current_writing_run_index = runs_count;
	std::vector <int> pointers_start_index;
	std::vector <int> pointers_end_index;
	string min_str; int min_index;	// for the minimum string
	int mem_blocks_left;
	int begin; int end; // IN PASS-1:	begin = index of first run to be read and merged  in runs_names
	int start;			//	end = index of last run to be read and merged in runs_names
	int blocks;			//	start = index of first run of series being merged

	for(int p = 0; p < runs_in_pass.size(); p++) {
	
		begin = current_reading_run_index;
		start = begin;
		end = current_writing_run_index - 1;
		cout << "PASS-" << p+1 << " is processing..." << endl;

		// while pass k is happening, and series of runs are being merged to generate more
		while(start <= end)	
		{
			// greater than or equal to memory_blocks - 1 runs are left to be merged
			if(((end+1)-start)/(memory_blocks-1) >= 1)	
				blocks = memory_blocks - 1;
			else										// less than memory_blocks - 1 runs are left to be merged
				blocks = ((end + 1) - start)%(memory_blocks-1);
			start += blocks; // increment start accordingly to set it to the appropriate index for next time

			// --------------BEGIN loading data---------------------		
			int word_counter = 0;
			for(int i = 0; i < blocks ; i++)
			{
				inFile.open(runs_names[current_reading_run_index].c_str());
				pointers_start_index.push_back(word_counter);
				// of the data in the ith memory block
				while(!inFile.eof())												
				{
					inFile >> word;
					data.push_back(word);
					word_counter++;
				}
				pointers_end_index.push_back(word_counter-1);
				inFile.close();	
				current_reading_run_index++;
			}
			// --------------END loading data---------------------
			// --------------BEGIN merging---------------------
			outFile.open(runs_names[current_writing_run_index].c_str());
			int sum_start = std::accumulate(pointers_start_index.begin(), pointers_start_index.end(),0);	// sum of elements in pointers_start_index
			int sum_end = std::accumulate(pointers_end_index.begin(), pointers_end_index.end(),0);			// sum of elements in pointers_end_index
			int current_pointer_index;
			while(sum_start != sum_end)
			{	
				// IF ALL DATA SEGMENTS HAVE NOT BEEN MERGED YET
				current_pointer_index = 0;
				// WHILE pointers_start_index and pointers_end_index vectors have the same value for the current_pointer_index, increment it
				while(pointers_start_index[current_pointer_index] == pointers_end_index[current_pointer_index])
					current_pointer_index++;
				// THIS DATA SEGMENT HAS NOT BEEN ITERATED THROUGH YET:
				min_str = data[pointers_start_index[current_pointer_index]];
				min_index = current_pointer_index;
				while(current_pointer_index < pointers_start_index.size())
				{
					if(pointers_start_index[current_pointer_index] == pointers_end_index[current_pointer_index])
						current_pointer_index++;
					else if(min_str.compare(data[pointers_start_index[current_pointer_index]]) > 0)
					{
						min_str = data[pointers_start_index[current_pointer_index]];
						min_index = current_pointer_index;
						current_pointer_index++;
					}
					else
						current_pointer_index++;
				}
				outFile << min_str;
				outFile << " ";
				pointers_start_index[min_index] = pointers_start_index[min_index] + 1;
				sum_start++;
			}
			outFile.close();
			current_writing_run_index++;
			data.clear();
			pointers_start_index.clear();
			pointers_end_index.clear();
				// --------------END merging---------------------
		}
		cout << "PASS-" << p+1 << " is now complete!" << endl << endl;
	}
	cout << endl;

	// TOTAL TEST SIZE OF RUN FILES:
/*	int run_sum_size = 0;
	int run_file_size=0;
	for(run_index = 0; run_index < runs_count; run_index++)
	{
		inFile.open(runs_names[run_index].c_str());
		inFile.seekg (0, inFile.end);
		run_file_size = inFile.tellg();
		inFile.seekg (0, inFile.beg);
		run_sum_size += run_file_size;
		inFile.close();
	}
	cout << "The sum of the sizes of each run file is: " << run_sum_size << endl;
*/

	// End Time
	cout << "Entire process took a total of: " << float(clock() - begin_time) / CLOCKS_PER_SEC * 1000 << " msec." << endl;

	// Size of the input file:
	inFile.open(input_file.c_str());
	inFile.seekg (0, inFile.end);
    int filesize = inFile.tellg();
    inFile.seekg (0, inFile.beg);
	cout << "The size of the input file, " <<  input_file << ", is: " << filesize << " bytes." << endl;
	inFile.close();

	// Size of the output file:
	inFile.open(output_file.c_str());
	inFile.seekg (0, inFile.end);
    filesize = inFile.tellg();
    inFile.seekg (0, inFile.beg);
	cout << "The size of the output file, " <<  output_file << ", is: " << filesize << " bytes." << endl;
	inFile.close();

	cout << "\n\n\n\n\n----------END OF PROGRAM HAS BEEN REACHED----------" << endl;
	int take;
	cin >> take;
	return 0;
}
